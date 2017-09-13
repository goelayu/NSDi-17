#!/usr/bin/env python

import errno
import sys
from os import listdir
import xml.etree.ElementTree as et
import random
import os

NET_LATENCIES_LOG = '/vault-home/goelayu/NSDI17/goelayu/with-missing-gc-reg/located-ping-times.txt'
STORE_LATENCIES_TOP = '/vault-home/uluyol/paxosstore-results/sm-100K/'
STORE_READ_FILE = '/store-reads.log'
STORE_WRITE_FILE = '/store-writes.log'

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


def ReadNetworkLatencies():
	"""
	Reads network latencies from the given file
	More information about file format inside /vault-home/uluyol/paxosstore-results/README
	"""
	print "Reading network latency file..."
	latencies = {}
	latencyFile = open(NET_LATENCIES_LOG, 'r')
	# errorCount = 0
	# valueCount = 0
	for line in latencyFile.readlines():
		lineData = line.strip().split()
		dc1 = lineData[1]
		dc2 = lineData[2]
		latency = lineData[0]


		if latency != "error":
			if dc1 not in latencies:
				latencies[dc1] = {}
			if dc2 not in latencies[dc1]:
				latencies[dc1][dc2] = []
			try:
				l = float(latency)
				latencies[dc1][dc2].append(l)
			except ValueError:
				pass
			# valueCount += 1

			# errorCount += 1
		# if dc1 == "az/northcentralus" and dc2 == "az/eastus":
		# 	print latency
		# 	print len(latencies[dc1][dc2])
	# print valueCount
	return latencies

def ReadStorageLatencies():
	"""
	Reads storage latencies from the given file
	More information about file format inside /vault-home/uluyol/paxosstore-results/README
	"""
	print "Reading storage latency file..."
	readLatency = {}
	writeLatency = {}
	datacenters = listdir(STORE_LATENCIES_TOP)
	for dc in datacenters:
		readStoragefile = open(STORE_LATENCIES_TOP + dc + STORE_READ_FILE, 'r')
		for line in readStoragefile.readlines():
			lineData = line.strip().split()
			status = lineData[0]
			latency = lineData[3]

			if status == "success":
				dcKey = dc.replace("-", "/", 1)
				if dcKey not in readLatency:
					readLatency[dcKey] = []
				try:
					l = float(latency)
					readLatency[dcKey].append(l)
				except ValueError:
					pass

		writeStoragefile = open(STORE_LATENCIES_TOP + dc + STORE_WRITE_FILE, 'r')
		for line in writeStoragefile.readlines():
			lineData = line.strip().split()
			status = lineData[0]
			latency = lineData[3]

			if status == "success":
				dcKey = dc.replace("-", "/", 1)
				if dcKey not in writeLatency:
					writeLatency[dcKey] = []
				try:
					l = float(latency)
					writeLatency[dcKey].append(l)
				except ValueError:
					pass

		# if dc == "gc-asia-east1":
		# 	print len(readLatency[dc])
		# 	print len(writeLatency[dc])
	return (readLatency, writeLatency)

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

def GetRequestGenerator(networkLatencies, readStorageLatencies, randomFrontEnd, listOfReplicas, readQourumSize):
	sampledNetworkLatencies = {}
	sampledStorageLatencies = {}
	totalLinkLatency = {}
	for dc in listOfReplicas:
		sampledNetworkLatencies[dc] = random.choice(networkLatencies[randomFrontEnd][dc])
		sampledStorageLatencies[dc] = random.choice(readStorageLatencies[dc])
		# print sampledNetworkLatencies[dc], sampledNetworkLatencies[dc]
		totalLinkLatency[dc] = float(sampledNetworkLatencies[dc]) + float(sampledStorageLatencies[dc])

	readQourum = [listOfReplicas[i] for i in random.sample(xrange(len(listOfReplicas)), readQourumSize)]

	getLatencies = [totalLinkLatency[i] for i in readQourum]

	getLatencies.sort()
	return getLatencies[-1]

def PutRequestGenerator(networkLatencies, storageLatencies, randomFrontEnd, listOfReplicas, readQourumSize, writeQourumSize):
	sampledNetworkLatencies = {}
	sampledStorageLatencies = {}
	totalLinkLatency = {}
	for dc in listOfReplicas:
		# print "network latency", networkLatencies[randomFrontEnd][dc][:10]
		sampledNetworkLatencies[dc] = random.choice(networkLatencies[randomFrontEnd][dc])
		# print "storage latency", storageLatencies[0][dc][:10]
		sampledStorageLatencies[dc] = random.choice(storageLatencies[0][dc])
		# print sampledNetworkLatencies[dc], sampledNetworkLatencies[dc]
		totalLinkLatency[dc] = float(sampledNetworkLatencies[dc]) + float(sampledStorageLatencies[dc])

	readQourum = [listOfReplicas[i] for i in random.sample(xrange(len(listOfReplicas)), readQourumSize)]
	getLatencies = [totalLinkLatency[i] for i in readQourum]
	getLatencies.sort()
	phase1Latency = getLatencies[-1]

	# clarify whether use the same links or not as the front end is the same
	sampledNetworkLatencies = {}
	sampledStorageLatencies = {}
	totalLinkLatency = {}
	for dc in listOfReplicas:
		sampledNetworkLatencies[dc] = random.choice(networkLatencies[randomFrontEnd][dc])
		sampledStorageLatencies[dc] = random.choice(storageLatencies[1][dc])
		# print sampledNetworkLatencies[dc], sampledNetworkLatencies[dc]
		totalLinkLatency[dc] = float(sampledNetworkLatencies[dc]) + float(sampledStorageLatencies[dc])

	writeQourum = [listOfReplicas[i] for i in random.sample(xrange(len(listOfReplicas)), writeQourumSize)]
	putLatencies = [totalLinkLatency[i] for i in writeQourum]
	putLatencies.sort()
	phase2Latency = putLatencies[-1]

	return phase1Latency + phase2Latency

def main():
	if len(sys.argv) < 3:
		print "Invalid argument"
		print "Usage: python compare_latencies.py <path to config file> <qourum system | -b,-p,-rd> [number of requests, default=50k]"
		return
	configFile = sys.argv[1]
	qourumSystem = sys.argv[2][1:]
	numberOfRequests = 50000
	if len(sys.argv) == 4:
		numberOfRequests = int(sys.argv[3])

	networkLatencies = ReadNetworkLatencies()
	storageLatencies = ReadStorageLatencies()
	readQourumSize = 0
	writeQourumSize = 0
	accessSet = []
	listOfReplicas = []
	parsedConfigFile = ReadConfigFile(configFile)
	for iter in parsedConfigFile:
		if len(iter[0]) >= 2:
			if iter[0][0] == "M" and iter[0][1] == "R":
				readQourumSize = iter[1]
			elif iter[0][0] == "M" and iter[0][1] == "W":
				writeQourumSize = iter[1]
			elif iter[0][0] == "C":
				if dcIndexMap[iter[0][1]] not in listOfReplicas:
					listOfReplicas.append(dcIndexMap[iter[0][1]])
			elif iter[0][0] == "R" or iter[0][0] == "W":
				if dcIndexMap[iter[0][1]] not in accessSet:
					accessSet.append(dcIndexMap[iter[0][1]])

	print "Read qourum size", readQourumSize
	print "Write qourum size", writeQourumSize
	print "Access set", accessSet
	print "List of replicas", listOfReplicas

	if qourumSystem == "b":
		outputFileName = 'results/' + 'latency_' + qourumSystem + '_' + str(numberOfRequests)
		if not os.path.exists(os.path.dirname(outputFileName)):
			try:
				os.makedirs(os.path.dirname(outputFileName))
			except OSError as exc: # Guard against race condition
				if exc.errno != errno.EEXIST:
					raise
		outputFile = open(outputFileName, 'w')
		for _ in range(numberOfRequests):
			frontend = random.choice(accessSet)
			sampleGet = GetRequestGenerator(networkLatencies, storageLatencies[0], frontend, listOfReplicas, readQourumSize)
			samplePut = PutRequestGenerator(networkLatencies, storageLatencies, frontend, listOfReplicas, readQourumSize, writeQourumSize)
			outputFile.write("get " + str(sampleGet) + " put " + str(samplePut) + "\n")



if __name__ == '__main__':
	main()
