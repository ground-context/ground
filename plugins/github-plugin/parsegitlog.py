#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
Receive repo name from kafka
Fetch repo commit history
Add any new commits to ground
"""

import json
import os
from collections import OrderedDict

import git
import requests
from kafka import KafkaConsumer
import configparser

# TODO manage the kafka offset for restarts, error handling


def get_commits(git_repo):
    parents_log = git_repo.log('--all', '--format="%H,%P"')  # log with commit hash and parent hashes
    parents_log = parents_log.replace("\"", "").split("\n")
    parents_log = [row.strip().split(",") for row in parents_log]
    parents_log[-1:] = [[parents_log[-1:][0][0], ""]]  # fix for first commit
    parents_log = [[row[0], row[1].split()] for row in parents_log]

    source_log = git_repo.log('--all', '--source', '--format=oneline')  # log with source branch of commit
    source_log = source_log.split("\n")
    source_log = [[row.split("\t")[0], row.split("\t")[1].split(" ")[0]] for row in source_log]

    source_log = [[row[1]] for row in source_log]
    fields = ['branch', 'commitHash', 'parentHashes']
    commits_dict = list(zip(source_log, parents_log))
    commits_dict = [[row[0][0], row[1][0], row[1][1]] for row in list(commits_dict)]
    commits_dict = [OrderedDict(zip(fields, row)) for row in commits_dict]
    firstCommit = commits_dict[-1:][0]
    i = len(commits_dict)
    for commit in commits_dict:
        commit.update({'num': i})
        i -= 1
    commits_dict = {a['commitHash']: {'parentHashes': a['parentHashes'],
                    'branch': a['branch'], 'num': int(a['num'])} for a in
                    commits_dict}
    return commits_dict


def get_new_commits(commits):
    latests = requests.get(latestUrl).text  # get latest commits # TODO get the latest commit

    # TODO do this with less looping. could just POST until we get to the latest node
    for latest in latests:  # can there be multiple latest versions?
        for item in commits:  # loop through all commits until you get to the latest one store in ground
            if latest == item:
                commits = commits[:commits.index(item)]  # remove everything before the latest commit









config = configparser.ConfigParser()
config.read('config.ini')
consumer = KafkaConsumer(config['Kafka']['topic'],
                         bootstrap_servers=[config['Kafka']['url'] + ":" + config['Kafka']['port']])
url = 'http://' + config['Ground']['url'] + ':' + config['Ground']['port']
nodeVUrl = url + '/nodes/versions/'

for msg in consumer:

    parsed_msg = json.loads(msg.value.decode("utf-8"))  # turn into json

    print("Message key/Repo name: "+msg.key.decode("utf-8"))  # print key

    gitUrl = parsed_msg['repository']['git_url']
    # gitUrl = 'git://github.kdc.capitalone.com/gryzzl/edt.git'
    print("Git URL: "+gitUrl)
    # repoId = msg.key.decode("utf-8")  # key from kakfa
    repoId = str(parsed_msg['repository']['id'])
    # create URLS for API interaction
    nodeUrl = url + '/nodes/' + repoId
    latestUrl = nodeUrl + '/latest'

    if not os.path.exists(config['Git']['path'] + repoId):
        repo = git.Repo.init(config['Git']['path'] + repoId, bare=True)
        origin = repo.create_remote('origin', url=gitUrl)
    else:
        repo = git.Repo(config['Git']['path'] + repoId)

    repo.remotes.origin.fetch()  # update repo metadata
    g = git.Git(config['Git']['path'] + repoId)

    commits = get_commits(g)


    latest_commits = get_new_commits(commits)





    if latests == '[]':  # TODO if this is a new to ground, create a node and a first node version
        print(latests)
        requests.post(nodeUrl)  # create initial node in ground
        nodeVData = {
            "tags": {
                "branch": {
                    "key": "branch",
                    "value": firstCommit['branch'],
                    "type": "string"
                },
                "commit": {
                    "key": "commit",
                    "value": firstCommit['commitHash'],
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
        r = requests.post(nodeVUrl, data=json.dumps(nodeVData), headers=headers)
        nodeId = r.json()['id']
        print(nodeId)
        commits_dict[firstCommit['commitHash']].update({'nodeId': nodeId})

    for commit in commits_dict:
        parents = commits_dict[commit]['parentHashes']
        parentNodes = []
        for parent in parents:
            print(commits_dict[parent])
            parentNode = commits_dict[parent]['nodeId']
            parentNodes = [parentNode if x == parent else x for x in parents]
        params = {'parents': parentNodes}

        nodeVData = {
            "tags": {
                "branch": {
                    "key": "branch",
                    "value": commits_dict[commit]['branch'],
                    "type": "string"
                },
                "commit": {
                    "key": "commit",
                    "value": commit,
                    "type": "string"
                },
            },
            "nodeId": "Nodes." + repoId
        }
        headers = {
            'content-type': "application/json"
        }
        r = requests.post(nodeVUrl, params=params, data=json.dumps(nodeVData), headers=headers)
        print(commit)
        nodeId = r.json()['id']
        commits_dict[commit].update({'nodeId': nodeId})
