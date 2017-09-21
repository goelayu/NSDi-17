#!/usr/bin/env python

# Storage cost , network bandwidth cost, per access cost
import operator
import xml.etree.ElementTree as et
import argparse
import random

STORAGE_COST = 0.08 # PER GB
TRANSACTION_READ_COST = 0.004  #PER 10000 TRANSACTIONS
TRANSACTION_WRITE_COST = 0.05 
BANDWIDTH_COST = 0.08
DATA_SIZE = 10

dcIndexMap = {
  '0': 'aws/ap-northeast-1',
  '1': 'aws/ap-northeast-2',
  '2': 'aws/ap-south-1',
  '3': 'aws/ap-southeast-1',
  '4': 'aws/ap-southeast-2',
  '5': 'aws/ca-central-1',
  '6': 'aws/eu-central-1',
  '7': 'aws/eu-west-1',
  '8': 'aws/eu-west-2',
  '9': 'aws/sa-east-1',
  '10': 'aws/us-east-1',
  '11': 'aws/us-east-2',
  '12': 'aws/us-west-1',
  '13': 'aws/us-west-2',
  '14': 'az/australiaeast',
  '15': 'az/australiasoutheast',
  '16': 'az/brazilsouth',
  '17': 'az/canadacentral',
  '18': 'az/canadaeast',
  '19': 'az/centralindia',
  '20': 'az/centralus',
  '21': 'az/eastasia',
  '22': 'az/eastus',
  '23': 'az/eastus2',
  '24': 'az/japaneast',
  '25': 'az/japanwest',
  '26': 'az/koreacentral',
  '27': 'az/koreasouth',
  '28': 'az/northcentralus',
  '29': 'az/northeurope',
  '30': 'az/southcentralus',
  '31': 'az/southeastasia',
  '32': 'az/southindia',
  '33': 'az/uksouth',
  '34': 'az/ukwest',
  '35': 'az/westcentralus',
  '36': 'az/westeurope',
  '37': 'az/westus',
  '38': 'az/westus2',
  '39': 'gc/asia-east1',
  '40': 'gc/asia-northeast1',
  '41': 'gc/asia-southeast1',
  '42': 'gc/europe-west1',
  '43': 'gc/us-central1',
  '44': 'gc/us-east1',
  '45': 'gc/us-west1',
}


def computeStorageLatencyReplication(listOfReplicas, numberOfSplits, splitSize):

    totalDataReplication = DATA_SIZE*len(listOfReplicas)

    return totalDataReplication*STORAGE_COST

def computeStorageLatencyECC(listOfReplicas, numberOfSplits, splitSize):

    totalDataECC = DATA_SIZE*len(listOfReplicas)/splitSize

    return  totalDataECC*STORAGE_COST

def computeStorageLatencyPanda(listOfReplicas, numberOfSplits, splitSize):

    totalSlots = 0
    for replica in listOfReplicas:
        totalSlots += numberOfSplits[replica]
    totalDataPanda = totalSlots*splitSize

    return totalDataPanda*STORAGE_COST

def ComputeTransactionCostReplication(listOfReplicas, nop, readQuorumSize, writeQourumSize):

    totalTransactionReplicationCost = 2*nop*writeQourumSize*TRANSACTION_WRITE_COST/10000 + nop*readQuorumSize*TRANSACTION_READ_COST/10000

    return totalTransactionReplicationCost

def ComputeTransactionCostECC(listOfReplicas, nop, readQuorumSize, writeQourumSize):

    totalTransactionECCCost = nop*writeQourumSize*TRANSACTION_WRITE_COST/10000 + nop*len(listOfReplicas)*TRANSACTION_WRITE_COST/10000 + nop*len(listOfReplicas)*TRANSACTION_READ_COST/10000

    return totalTransactionECCCost

def ComputeTransactionCostPanda(listOfReplicas, nop, readQuorumSize, writeQourumSize):

    totalTransactionPandaCost = nop*readQuorumSize*TRANSACTION_READ_COST/10000 +  nop*len(listOfReplicas)*TRANSACTION_WRITE_COST/10000 + nop*len(listOfReplicas)*TRANSACTION_READ_COST/10000

    return totalTransactionPandaCost

def ReadConfigFile(solution_file):
    tree = et.parse(solution_file)
    root = tree.getroot()
    child = None
    for elem in root:
        if elem.tag == "variables":
            child = elem
            break
    for line in child:
        val = int(float(line.attrib["value"])+0.5)
        if val != 0:
            items = line.attrib["name"].split("_")
            yield tuple(items), val

def main():
    # make execution deterministic
    random.seed(0)
    parser = argparse.ArgumentParser()
    parser.add_argument("--op-count", default=50000, type=int)
    parser.add_argument("configpath")
    args = parser.parse_args()

    configFile = args.configpath
    numberOfRequests = args.op_count

    readQuorumSize = 0
    writeQuorumSize = 0
    accessSet = []
    listOfReplicas = []
    numberOfSplits = {}
    splitSize = 0
    specificReadQuorums = [None] * 46 #Since 46 DCs
    specificWriteQuorums = [None] * 46 #Same reason

    if "fixautosplit" in configFile or "splitsasym-auto" in configFile:
        quorumSystem = "basic"
    elif "flexgen" in configFile:
        quorumSystem = "spec"
    else:
        print "Error: Invalid nomenclature for the config file name"
        return

    parsedConfigFile = ReadConfigFile(configFile)

    for iter in parsedConfigFile:
        if len(iter[0]) == 2 and iter[0][0] == "M" and iter[0][1] == "R":
            readQuorumSize = iter[1]
        elif len(iter[0]) == 2 and iter[0][0] == "M" and iter[0][1] == "W":
            writeQuorumSize = iter[1]
        elif len(iter[0]) == 2 and iter[0][0] == "C":
            if dcIndexMap[iter[0][1]] not in listOfReplicas:
                listOfReplicas.append(dcIndexMap[iter[0][1]])
        elif len(iter[0]) == 3 and iter[0][0] == "R":
            if dcIndexMap[iter[0][1]] not in accessSet:
                accessSet.append(dcIndexMap[iter[0][1]])
            if not specificReadQuorums[int(iter[0][1])]:
                specificReadQuorums[int(iter[0][1])] = []
            if dcIndexMap[iter[0][2]] not in specificReadQuorums[int(iter[0][1])]:
                specificReadQuorums[int(iter[0][1])].append(dcIndexMap[iter[0][2]])
        elif len(iter[0]) == 3 and iter[0][0] == "W":
            if dcIndexMap[iter[0][1]] not in accessSet:
                accessSet.append(dcIndexMap[iter[0][1]])
            if not specificWriteQuorums[int(iter[0][1])]:
                specificWriteQuorums[int(iter[0][1])] = []
            if dcIndexMap[iter[0][2]] not in specificWriteQuorums[int(iter[0][1])]:
                specificWriteQuorums[int(iter[0][1])].append(dcIndexMap[iter[0][2]])
        elif len(iter[0]) == 2 and iter[0][0] == "SPLITS":
            numberOfSplits[dcIndexMap[iter[0][1]]] = iter[1]
        elif len(iter[0]) == 1 and iter[0][0] == "NSPLITS":
            splitSize = iter[1]

    specificReadQuorums = [q for q in specificReadQuorums if q]
    specificWriteQuorums = [q for q in specificWriteQuorums if q]

    # print "Read quorum size", readQuorumSize
    # print "Write quorum size", writeQuorumSize
    # print "Access set", accessSet
    # print "List of replicas", listOfReplicas
    # print "read quorums", specificReadQuorums
    # print "write quorums", specificWriteQuorums

    storageCost = 0
    transactionCost = 0
    if quorumSystem == "basic" and splitSize == 1:
        storageCost = computeStorageLatencyReplication(listOfReplicas, numberOfSplits, splitSize)
        transactionCost = ComputeTransactionCostReplication(listOfReplicas, numberOfRequests, readQuorumSize, writeQuorumSize)
    elif quorumSystem == "basic" and splitSize != 1:
        storageCost = computeStorageLatencyECC(listOfReplicas, numberOfSplits, splitSize)
        transactionCost = ComputeTransactionCostECC(listOfReplicas, numberOfRequests, readQuorumSize, writeQuorumSize)
    elif quorumSystem == "spec":
        storageCost = computeStorageLatencyPanda(listOfReplicas, numberOfSplits, splitSize)
        transactionCost = ComputeTransactionCostPanda(listOfReplicas, numberOfRequests, len(specificReadQuorums[0]), len(specificWriteQuorums[0]))

    print storageCost + transactionCost

if __name__ == '__main__':
    main()