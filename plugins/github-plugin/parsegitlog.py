#!/usr/local/bin/python3

import json
import os

import git
import requests
from kafka import KafkaConsumer
import configparser

# TODO make this faster
# TODO kafka params
config = configparser.ConfigParser()
config.read('config.ini')
consumer = KafkaConsumer(config['Kafka']['topic'], bootstrap_servers=[config['Kafka']['url']+":"+config['Kafka']['port']])

for msg in consumer:
    # print (msg)
    parsed_msg = json.loads(msg.value.decode("utf-8"))

    print(msg.key.decode("utf-8"))
    print(parsed_msg['repository']['git_url'])

    # gitUrl = "git@github.com:ground-context/ground.git"
    gitUrl = parsed_msg['repository']['git_url']  # value from kafka > into json
    #gitUrl = 'https://github.kdc.capitalone.com/gryzzl/cartographer.git'
    # repoId = "12341234"
    repoId = msg.key.decode("utf-8")  # key from kakfa

    if not os.path.exists(repoId):
        # os.makedirs(repoId)
        # os.chdir(repoId)
        repo = git.Repo.init(repoId, bare=True)
        origin = repo.create_remote('origin', url=gitUrl)
        # origin = repo.create_remote('origin', url='git@github.com:ground-context/ground.git')
        # self("git remote add -f origin "+gitUrl)
    else:
        # os.chdir(repoId)
        repo = git.Repo(repoId)
        # origin = repo.remotes.origin

    # TODO get most recent commit from ground, get commits that have happened since
    repo.remotes.origin.fetch()  # update repo metadata, change to fetch --all, but not important
    g = git.Git(repoId)

    l0 = g.log('--all', '--format="%H,%P"')
    l1 = l0.replace("\"", "").split("\n")
    l2 = [row.strip().split(",") for row in l1]
    l2[-1:] = [[l2[-1:][0][0], ""]]
    l3 = [[row[0], row[1].split()] for row in l2]

    r0 = g.log('--all', '--source', '--format=oneline')
    r1 = r0.split("\n")
    r2 = [row.split("\t")[1] for row in r1]
    r3 = [[row.split(" ")[0]] for row in r2]

    t = list(zip(r3, l3))
    t1 = [[row[0][0], row[1][0], row[1][1]] for row in list(t)]
    GIT_COMMIT_FIELDS = ['branch', 'commitHash', 'parentHashes']
    di = [dict(zip(GIT_COMMIT_FIELDS, row)) for row in t1]


    for commit in di:
        print(commit['branch'])
        # for each commit, create a node version. include the parents of the commit
        # ?first=1&second=12&third=5




    lineageName = 'lineageTest'

    nodeUrl = 'localhost:9090/nodes/' + repoId
    nodeVUrl = 'localhost:9090/nodes/versions/'
    edgeUrl = 'localhost:9090/edges/' + lineageName
    edgeVUrl = 'localhost:9090/edges/versions'

    nodeData = {
        "id": "Nodes." + repoId,
        "name": repoId
    }

    nodeVData = {
        "id": "a0bb1ca6876a6fd69862b9519489b62ccfa7f3f8",
        "tags": {
            "testtag": {
                "versionId": "a0bb1ca6876a6fd69862b9519489b62ccfa7f3f8",
                "key": "branch",
                "value": "master",
                "type": "string"
            }
        },
        "nodeId": "Nodes.ground"
    }

    edgeData = {
        "id": "Edges." + repoId,
        "name": repoId
    }

    edgeVData = {
        "id": "abcd",
        "tags": {
            "testtag": {
                "versionId": "abcd",
                "key": "testtag",
                "value": "tag",
                "type": "string"
            }
        },

        "reference": "http://www.google.com",
        "parameters": {
            "http": "GET"
        },

        "lineageEdgeId": "LineageEdges.test",

        "fromId": "123",

        "toId": "456"
    }

    #r = requests.post(url, data=json.dumps(data), )














# for each repo ref
# repo.refs  # gives all the refs
#
# for commit in git.objects.commit.Commit.iter_items(repo=r,
#                                                    rev='refs/remotes/origin/master',
#                                                    max_count=5):
#     print(commit)
#
# for commit in r.iter_commits('refs/remotes/origin/master', max_count=5):
#     print("commit: ", commit)
#     for parent in commit.iter_parents():
#         print("parent: ", parent)

# git --no-pager log --no-color --all --source --format=oneline
# 28347a5c3f84b6e5bcff130e4c88b60f1f1a7713  refs/remotes/origin/master Add README
#
# git rev-list --remotes --parents --all
# 9b08df6f57603e57f562427b6ecb870ae04256a9 96d4c4b88d7c10df373024beb8e4a7ca237c32f8 7c68804f0875d97b7b44d4ccb554e9362ffb9eb4
# Pretend as if all the refs in refs/ are listed on the command line as <commit>.
# git rev-list --all #lists all commits

# r.git.log()
# needs to be on a branch
# checkout master?
# start at very first commit
# find all commits that dont have children, get all the parents of all the commits

# g = git.Git("/Users/gfz081/Desktop/bare-repo")
# g.log('--all', max_parents=0) #get the very first commit
# g.log('--all', '--branches=*') #get all commits

# counting commits
# sum(1 for _ in repo.iter_commits())
# O(n)

# lists all commits
# git rev-list --remotes

# https://docs.python.org/3/library/subprocess.html#subprocess.run
# subprocess.run

# print(json.dumps(log, indent=4))
