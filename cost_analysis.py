#!/usr/bin/env python

# Storage cost , network bandwidth cost, per access cost
import xml.etree.ElementTree as et
import argparse

class CostConfig(object):
    def __init__(self, storagePerGB=-1.0, opReadPer10K=-1.0, opWritePer10K=-1.0, bwPerGB=-1.0):
        self.storagePerGB = storagePerGB
        self.opReadPer10K = opReadPer10K
        self.opWritePer10K = opWritePer10K
        self.bwPerGB = bwPerGB

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

def computeStorage(costs, dataSizeGB, listOfReplicas, numberOfSplits, splitSize, numSplitsOverall):

    totalDataECC = numSplitsOverall*dataSizeGB/splitSize

    return  totalDataECC*costs.storagePerGB

def ComputeBandwidthCostECC(costs, sizePerRequestGB, listOfReplicas, nop, readQuorumSize, writeQuorumSize, splitSize, getFrac):

    bw = nop * (1-getFrac) * writeQuorumSize + nop * (1-getFrac) * len(listOfReplicas)
    bw += nop * getFrac * readQuorumSize

    return bw * sizePerRequestGB * costs.bwPerGB / float(splitSize)

def ComputeBandwidthCostPando(costs, sizePerRequestGB, listOfReplicas, nop, readQuorumSize, writeQuorumSize, splitSize, getFrac):

    bw = nop * (1-getFrac) * readQuorumSize + nop * getFrac * len(listOfReplicas)
    bw += nop * getFrac * readQuorumSize

    return bw * sizePerRequestGB * costs.bwPerGB / float(splitSize)

def ComputeTransactionCostECC(costs, listOfReplicas, nop, readQuorumSize, writeQuorumSize, getFrac):

    c = nop*(1-getFrac)*writeQuorumSize*costs.opWritePer10K/10000
    c += nop*(1-getFrac)*len(listOfReplicas)*costs.opWritePer10K/10000
    c += nop*getFrac*readQuorumSize*costs.opReadPer10K/10000

    return c

def ComputeTransactionCostPando(costs, listOfReplicas, nop, readQuorumSize, writeQuorumSize, getFrac):

    c = nop*(1-getFrac)*readQuorumSize*costs.opWritePer10K/10000
    c += nop*(1-getFrac)*len(listOfReplicas)*costs.opWritePer10K/10000
    c += nop*getFrac*readQuorumSize*costs.opReadPer10K/10000

    return c

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage-per-gb", type=float, default=0.0208)
    parser.add_argument("--op-read-per-10k", type=float, default=0.004)
    parser.add_argument("--op-write-per-10k", type=float, default=0.05)
    parser.add_argument("--bw-per-gb", type=float, default=0.087)
    parser.add_argument("--db-size-gb", type=float, default=100)
    parser.add_argument("--value-size-bytes", type=float, default=1024)
    parser.add_argument("--op-count", default=50000, type=int)
    parser.add_argument("--gp-ratio", default=1, type=float)
    parser.add_argument("configpath")
    args = parser.parse_args()

    costConfig = CostConfig(
	  storagePerGB=args.storage_per_gb,
	  opReadPer10K=args.op_read_per_10k,
	  opWritePer10K=args.op_write_per_10k,
	  bwPerGB=args.bw_per_gb)

    configFile = args.configpath
    numberOfRequests = args.op_count
    dataSizeGB = args.db_size_gb
    valueSizeGB = args.value_size_bytes / float(2**30)
    getFrac = args.gp_ratio / (args.gp_ratio + 1.0)

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
        elif len(iter[0]) == 1 and iter[0][0] == "NREPLICAS":
            numSplitsOverall = iter[1]

    specificReadQuorums = [q for q in specificReadQuorums if q]
    specificWriteQuorums = [q for q in specificWriteQuorums if q]

    # print "Read quorum size", readQuorumSize
    # print "Write quorum size", writeQuorumSize
    # print "Access set", accessSet
    # print "List of replicas", listOfReplicas
    # print "read quorums", specificReadQuorums
    # print "write quorums", specificWriteQuorums
    # print "Split size", splitSize
    # print "Num splits", numSplitsOverall

    storageCost = 0
    transactionCost = 0
    if quorumSystem == "basic":
        storageCost = computeStorage(costConfig, dataSizeGB, listOfReplicas, numberOfSplits, splitSize, numSplitsOverall)
        transactionCost = ComputeTransactionCostECC(costConfig, listOfReplicas, numberOfRequests, readQuorumSize, writeQuorumSize, getFrac)
        bandWidthCost = ComputeBandwidthCostECC(costConfig, valueSizeGB, listOfReplicas, numberOfRequests, readQuorumSize, writeQuorumSize, splitSize, getFrac)
    elif quorumSystem == "spec":
        storageCost = computeStorage(costConfig, dataSizeGB, listOfReplicas, numberOfSplits, splitSize, numSplitsOverall)
        transactionCost = 0
        bandWidthCost = 0
        assert len(specificReadQuorums) == len(specificWriteQuorums)
        for i in range(len(specificReadQuorums)):
            numReq = numberOfRequests / float(len(specificReadQuorums))
            transactionCost += ComputeTransactionCostPando(costConfig, listOfReplicas, numReq, len(specificReadQuorums[i]), len(specificWriteQuorums[i]), getFrac)
            bandWidthCost += ComputeBandwidthCostPando(costConfig, valueSizeGB, listOfReplicas, numReq, len(specificReadQuorums[i]), len(specificWriteQuorums[i]), splitSize, getFrac)

    #print storageCost, " +", transactionCost, " +", bandWidthCost, "=", storageCost + transactionCost + bandWidthCost
    print "{0},{1}".format("Storage", storageCost)
    print "{0},{1}".format("Request", transactionCost)
    print "{0},{1}".format("Network", bandWidthCost)

if __name__ == '__main__':
  main()
