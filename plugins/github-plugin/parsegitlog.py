'''
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

"""
Receive repo name/url from kafka
Fetch repo commit history
Add any new commits to ground
"""

import json
import os

import git
import requests
from kafka import KafkaConsumer
import configparser


# TODO manage the kafka offset for restarts
# TODO Add a logger
# TODO fails if a commit has a parent that is not in 'latest' list
# TODO should repo id be the name instead


def get_commits(git_repo, latest_nodes):
    parents_log = git_repo.log('--all',
                               '--format="%H,%P"')  # log with commit hash and parent hashes
    parents_log = parents_log.replace("\"", "").split("\n")
    parents_log = [row.strip().split(",") for row in parents_log]
    parents_log[-1:] = [[parents_log[-1:][0][0], ""]]  # fix for first commit
    parents_log = [[row[0], row[1].split()] for row in parents_log]
    source_log = git_repo.log('--all', '--source',
                              '--format=oneline')  # log with source branch of commit
    source_log = source_log.split("\n")
    source_log = [[row.split("\t")[0], row.split("\t")[1].split(" ")[0]] for row in source_log]

    for c_hash, node in latest_nodes.items():  # remove commits from before the latest commit
        for item in source_log:
            if c_hash == item[0]:
                source_log = source_log[:source_log.index(item)]
        for item in parents_log:
            if c_hash == item[0]:
                parents_log = parents_log[:parents_log.index(item)]

    source_log = [[row[1]] for row in source_log]
    fields = ['branch', 'commitHash', 'parentHashes']
    commits_dict = list(zip(source_log, parents_log))
    commits_dict = [[row[0][0], row[1][0], row[1][1]] for row in list(commits_dict)]
    commits_dict = [dict(zip(fields, row)) for row in commits_dict]
    return commits_dict


def latest_commits(repo_id):
    latests_url = url + '/nodes/' + repo_id + '/latest'
    node_url = url + '/nodes/versions/'
    print('getting latest commits from ground...')
    node_ids = requests.get(latests_url).json()  # send the request
    print('latest commits received from ground.')
    hashes = {}
    for node_id in node_ids:
        commit_hash = requests.get(node_url + node_id).json()['tags']['commit']['value']
        hashes[commit_hash] = node_id

    return hashes


def post_commits(commits, latest_nodes):
    node_ids = latest_nodes
    # i = 0
    for c in reversed(commits):  # start at the first commmit
        params = {}
        parents = c['parentHashes']  # what are the parent hashes
        if len(parents) != 0:  # check if there are parents to this commit
            parent_nodes = []
            for parent in parents:
                parent_nodes.append(node_ids[parent])
            params = {'parents': parent_nodes}  # create params for the request
        node_version_data = {
            "tags": {
                "branch": {
                    "key": "branch",
                    "value": c['branch'],
                    "type": "string"
                },
                "commit": {
                    "key": "commit",
                    "value": c['commitHash'],
                    "type": "string"
                },
            },
            "structureVersionId": None,
            "reference": None,
            "nodeId": "Nodes." + repoId,
            "parameters": {}
        }
        headers = {
            'content-type': "application/json"
        }
        r = requests.post(nodeVUrl, params=params, data=json.dumps(node_version_data),
                          headers=headers)  # send the request
        print('Commit added to ground: ' + c['commitHash'])
        node_ids[c['commitHash']] = r.json()['id']  # map ground's node id with the commit hash


config = configparser.ConfigParser()
config.read('config.ini')
consumer = KafkaConsumer(config['Kafka']['topic'],
                         bootstrap_servers=[config['Kafka']['url'] + ":" + config['Kafka']['port']])
url = 'http://' + config['Ground']['url'] + ':' + config['Ground']['port']
nodeVUrl = url + '/nodes/versions/'

for msg in consumer:

    parsed_msg = json.loads(msg.value.decode("utf-8"))  # convert to json
    print("Received repo: " + msg.key.decode("utf-8"))  # print key
    gitUrl = parsed_msg['repository']['git_url']
    repoId = msg.key.decode("utf-8")  # key from kakfa
    # repoId = str(parsed_msg['repository']['id'])  # do we want repo id or name?

    # create URLS for API interaction
    nodeUrl = url + '/nodes/' + repoId
    latestUrl = nodeUrl + '/latest'
    latests = {}

    if not os.path.exists(config['Git']['path'] + repoId):
        repo = git.Repo.init(config['Git']['path'] + repoId, bare=True)
        origin = repo.create_remote('origin', url=gitUrl)
    else:
        repo = git.Repo(config['Git']['path'] + repoId)
        latests = latest_commits(repoId)


    class MyProgressPrinter(git.RemoteProgress):
        def update(self, op_code, cur_count, max_count=None, message=''):
            print(op_code, cur_count, max_count, cur_count / (max_count or 100.0),
                  message or "NO MESSAGE")
            # end


    print('fetching commits....')
    for fetch_info in repo.remotes.origin.fetch(progress=MyProgressPrinter()):
        print("Updated %s to %s" % (fetch_info.ref, fetch_info.commit))
    print('commits fetched')
    g = git.Git(config['Git']['path'] + repoId)

    if not bool(latests):
        requests.post(nodeUrl)  # create initial node

    repo_commits = get_commits(g, latests)  # Get a list of the latest commits
    post_commits(repo_commits, latests)
