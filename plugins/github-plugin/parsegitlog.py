#!/usr/local/bin/python3

import json
import os
from collections import OrderedDict

import git
import requests
from kafka import KafkaConsumer
import configparser

# TODO manage the kafka offset for restarts
config = configparser.ConfigParser()
config.read('config.ini')
# consumer = KafkaConsumer('github')
consumer = KafkaConsumer(config['Kafka']['topic'],
                         bootstrap_servers=[config['Kafka']['url'] + ":" + config['Kafka']['port']])
url = 'http://' + config['Ground']['url'] + ':' + config['Ground']['port']

for msg in consumer:

    parsed_msg = json.loads(msg.value.decode("utf-8"))  # turn into json

    print(msg.key.decode("utf-8"))  # print key

    gitUrl = parsed_msg['repository']['git_url']
    # gitUrl = 'git://github.kdc.capitalone.com/gryzzl/edt.git'
    print(gitUrl)
    # repoId = msg.key.decode("utf-8")  # key from kakfa
    repoId = str(parsed_msg['repository']['id'])

    if not os.path.exists(config['Git']['path'] + repoId):
        repo = git.Repo.init(config['Git']['path'] + repoId, bare=True)
        origin = repo.create_remote('origin', url=gitUrl)
    else:
        repo = git.Repo(config['Git']['path'] + repoId)

    repo.remotes.origin.fetch()  # update repo metadata
    g = git.Git(config['Git']['path'] + repoId)

    # URLs
    nodeUrl = url + '/nodes/' + repoId
    nodeVUrl = url + '/nodes/versions/'
    latestUrl = nodeUrl + '/latest'

    parentsLog = g.log('--all', '--format="%H,%P"')  # log with commit hash and parent hashes
    parentsLog = parentsLog.replace("\"", "").split("\n")
    parentsLog = [row.strip().split(",") for row in parentsLog]
    parentsLog[-1:] = [[parentsLog[-1:][0][0], ""]]  # fix for first commit
    parentsLog = [[row[0], row[1].split()] for row in parentsLog]

    sourceLog = g.log('--all', '--source', '--format=oneline')  # log with source branch of commit
    sourceLog = sourceLog.split("\n")
    sourceLog = [[row.split("\t")[0], row.split("\t")[1].split(" ")[0]] for row in sourceLog]

    latests = requests.get(latestUrl)  # get latest commits
    # latests = ['91f8afaae21b6a360ac1e150ddbca342bdb4edfd']
    for latest in latests:  # remove commits from before the latest commit
        for item in sourceLog:
            if latest == item[0]:
                sourceLog = sourceLog[:sourceLog.index(item)]
        for item in parentsLog:
            if latest == item[0]:
                parentsLog = parentsLog[:parentsLog.index(item)]

    sourceLog = [[row[1]] for row in sourceLog]
    GIT_COMMIT_FIELDS = ['branch', 'commitHash', 'parentHashes']
    commitsDict = list(zip(sourceLog, parentsLog))
    commitsDict = [[row[0][0], row[1][0], row[1][1]] for row in list(commitsDict)]
    commitsDict = [OrderedDict(zip(GIT_COMMIT_FIELDS, row)) for row in commitsDict]
    firstCommit = commitsDict[-1:][0]
    # reorganize dict
    commitsDict = {a['commitHash']: {'parentHashes': a['parentHashes'], 'branch': a['branch']}
                   for a in commitsDict}
    # TODO this needs to be an OrderedDict

    if latests == '[]':
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
            "nodeId": "Nodes." + repoId
        }
        r = requests.post(nodeVUrl, data=json.dumps(nodeVData))  # create v1 of node

    for commit in commitsDict:
        parents = commitsDict[commit]['parentHashes']
        parentNodes = []
        for parent in parents:
            parentNode = requests.get(nodeUrl)  # TODO get the parent node id
            parentNodes = [parentNode if x == parent else x for x in parents]
        params = {'parents': parentNodes}

        nodeVData = {
            "tags": {
                "branch": {
                    "key": "branch",
                    "value": commitsDict[commit]['branch'],
                    "type": "string"
                },
                "commit": {
                    "key": "commit",
                    "value": commitsDict[commit]['commitHash'],
                    "type": "string"
                },
            },
            "nodeId": "Nodes." + repoId
        }

        requests.post(nodeVUrl, params=params, data=json.dumps(nodeVData))
