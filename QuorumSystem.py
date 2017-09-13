#!/usr/bin/env python

import random

class Quorumsystem(object):


    def __init__(self, type, networkLatencies, storageLatencies, accessSet, replicas, readQuorumSize, writeQuorumSize, useFlexiblePaxos):
        self.type = type
        self.networkLatencies = networkLatencies
        self.storageLatencies = storageLatencies
        self.accessSet = accessSet
        self.replicas = replicas
        self.readQuorumSize = readQuorumSize
        self.writeQuorumSize = writeQuorumSize
        self.useFlexiblePaxos = useFlexiblePaxos

    def issueRequest(self, requestType):
        if self.type == "b":
            if requestType == "get":
                return BasicGetRequestGenerator(self.networkLatencies, self.storageLatencies[0], random.choice(self.accessSet), self.replicas, self.readQuorumSize)
            elif requestType == "put":
                return BasicPutRequestGenerator(self.networkLatencies, self.storageLatencies, random.choice(self.accessSet), self.replicas, self.readQuorumSize, self.writeQuorumSize, self.useFlexiblePaxos)


def BasicGetRequestGenerator(networkLatencies, readStorageLatencies, randomFrontEnd, listOfReplicas, readQuorumSize):
    sampledNetworkLatencies = {}
    sampledStorageLatencies = {}
    totalLinkLatency = {}
    for dc in listOfReplicas:
        sampledNetworkLatencies[dc] = random.choice(networkLatencies[randomFrontEnd][dc])
        sampledStorageLatencies[dc] = random.choice(readStorageLatencies[dc])
        # print sampledNetworkLatencies[dc], sampledNetworkLatencies[dc]
        totalLinkLatency[dc] = float(sampledNetworkLatencies[dc]) + float(sampledStorageLatencies[dc])

    readQuorum = [listOfReplicas[i] for i in random.sample(xrange(len(listOfReplicas)), readQuorumSize)]

    getLatencies = [totalLinkLatency[i] for i in readQuorum]

    getLatencies.sort()
    return getLatencies[-1]

def BasicPutRequestGenerator(networkLatencies, storageLatencies, randomFrontEnd, listOfReplicas, readQuorumSize, writeQuorumSize, useFlexiblePaxos):
    sampledNetworkLatencies = {}
    sampledStorageLatencies = {}
    totalLinkLatency = {}

    if useFlexiblePaxos:
        for dc in listOfReplicas:
            # print "network latency", networkLatencies[randomFrontEnd][dc][:10]
            sampledNetworkLatencies[dc] = random.choice(networkLatencies[randomFrontEnd][dc])
            # print "storage latency", storageLatencies[0][dc][:10]
            sampledStorageLatencies[dc] = random.choice(storageLatencies[0][dc])
            # print sampledNetworkLatencies[dc], sampledNetworkLatencies[dc]
            totalLinkLatency[dc] = float(sampledNetworkLatencies[dc]) + float(sampledStorageLatencies[dc])

        readQuorum = [listOfReplicas[i] for i in random.sample(xrange(len(listOfReplicas)), readQuorumSize)]
        getLatencies = [totalLinkLatency[i] for i in readQuorum]
        getLatencies.sort()
        phase1Latency = getLatencies[-1]

    else:
        for dc in listOfReplicas:
            # print "network latency", networkLatencies[randomFrontEnd][dc][:10]
            sampledNetworkLatencies[dc] = random.choice(networkLatencies[randomFrontEnd][dc])
            # print "storage latency", storageLatencies[0][dc][:10]
            sampledStorageLatencies[dc] = random.choice(storageLatencies[1][dc])
            # print sampledNetworkLatencies[dc], sampledNetworkLatencies[dc]
            totalLinkLatency[dc] = float(sampledNetworkLatencies[dc]) + float(sampledStorageLatencies[dc])

        writeQuorum = [listOfReplicas[i] for i in random.sample(xrange(len(listOfReplicas)), writeQuorumSize)]
        putLatencies = [totalLinkLatency[i] for i in writeQuorum]
        putLatencies.sort()
        phase1Latency = putLatencies[-1]

    # clarify whether use the same links or not as the front end is the same
    sampledNetworkLatencies = {}
    sampledStorageLatencies = {}
    totalLinkLatency = {}
    for dc in listOfReplicas:
        sampledNetworkLatencies[dc] = random.choice(networkLatencies[randomFrontEnd][dc])
        sampledStorageLatencies[dc] = random.choice(storageLatencies[1][dc])
        # print sampledNetworkLatencies[dc], sampledNetworkLatencies[dc]
        totalLinkLatency[dc] = float(sampledNetworkLatencies[dc]) + float(sampledStorageLatencies[dc])

    writeQuorum = [listOfReplicas[i] for i in random.sample(xrange(len(listOfReplicas)), writeQuorumSize)]
    putLatencies = [totalLinkLatency[i] for i in writeQuorum]
    putLatencies.sort()
    phase2Latency = putLatencies[-1]

    return phase1Latency + phase2Latency

