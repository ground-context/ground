#!/usr/local/bin/python3

import json
import os

import git
import requests
from kafka import KafkaConsumer

# TODO make this faster
# TODO kafka params
consumer = KafkaConsumer('github')  # name of topic to consume
for msg in consumer:
    # print (msg)
    parsed_msg = json.loads(msg.value.decode("utf-8"))

    print(msg.key.decode("utf-8"))
    print(parsed_msg['repository']['git_url'])

    # gitUrl = "git@github.com:ground-context/ground.git"
    gitUrl = parsed_msg['repository']['git_url']  # value from kafka > into json
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

    # TODO only need branch committed to
    repo.remotes.origin.fetch()  # update repo metadata, change to fetch --all, but not important
    g = git.Git(repoId)

    # parse the log and turn it into a dict
    log = g.log('--all', '--format="%H%x1f%P%x1e"')  # get all commits
    log = log.replace("\"\n\"", "").replace("\"", "").strip('\n\x1e').split(
        "\x1e")  # removes extra chars, split by row delimiter
    log = [row.strip().split("\x1f") for row in
           log]  # remove whitespace, split by field delimiter

    for row in log:
        if len(row) == 2:
            row[1].split()
            log is

    GIT_COMMIT_FIELDS = ['commitHash', 'parentHashes']
    log = [dict(zip(GIT_COMMIT_FIELDS, row)) for row in log]
    print(log)

    lineageName = 'lineageTest'

    nodeUrl = 'localhost:9090/nodes/' + repoId
    nodeVUrl = 'localhost:9090/nodes/versions/'
    lineageUrl = 'localhost:9090/lineage/' + lineageName
    lineageVUrl = 'localhost:9090/lineage/versions'

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

    lineageVData = {
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

    r = requests.post(url, data=json.dumps(data), )


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
