#!/usr/bin/env python

import errno
from os import listdir
import xml.etree.ElementTree as et
import random
import os
import argparse

NET_LATENCIES_LOG = '/vault-home/goelayu/NSDI17/goelayu/with-missing-gc-reg/located-ping-times.txt'
#NET_LATENCIES_LOG = '/w/uluyol/located-ping-times.txt'
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

class QuorumSystem(object):
	def readLatency(self, replicaNames, replicaLatencies):
		raise NotImplementedError

	def writeLatency(self, replicaNames, replicaLatencies):
		raise NotImplementedError

	def useReqPerSplit(self):
		raise NotImplementedError

class BasicQuorumSystem(object):
	def __init__(self, qrSize, qwSize):
		self._readQuorumSize = qrSize
		self._writeQuorumSize = qwSize

	def readLatency(self, _, replicaLatencies):
		replicaLatencies.sort()
		return replicaLatencies[self._readQuorumSize-1]

	def writeLatency(self, _, replicaLatencies):
		replicaLatencies.sort()
		return replicaLatencies[self._writeQuorumSize-1]

	def useReqPerSplit(self):
		return False

class SpecifiedQuorumSystem(object):
	def __init__(self, readQuorums, writeQuorums):
		self._readQuorums = readQuorums # type: List[Set[str]]
		self._writeQuorums = writeQuorums # type: List[Set[str]]

	def readLatency(self, replicaNames, replicaLatencies):
		qlats = []
		for _ in self._readQuorums:
			qlats.append(-1.0)
		for i in range(len(replicaNames)):
			n = replicaNames[i]
			for j in range(len(self._readQuorums)):
				if self._readQuorums[j] and n in self._readQuorums[j]:
					qlats[j] = max(qlats[j], replicaLatencies[i])
		return min(l for l in qlats if l >= 0)

	def writeLatency(self, replicaNames, replicaLatencies):
		qlats = []
		for _ in self._writeQuorums:
			qlats.append(-1.0)
		for i in range(len(replicaNames)):
			n = replicaNames[i]
			for j in range(len(self._writeQuorums)):
				if self._writeQuorums[j] and n in self._writeQuorums[j]:
					qlats[j] = max(qlats[j], replicaLatencies[i])
		return min(l for l in qlats if l >= 0)

	def useReqPerSplit(self):
		return False

class SpecifiedRPSQuorumSystem(object):
	def __init__(self, splitsPerReplica, readQuorums, writeQuorums, readExtra, writeExtra):
		self._readQuorums = readQuorums # type: List[Set[str]]
		self._writeQuorums = writeQuorums # type: List[Set[str]]
		self._readExtra = readExtra
		self._writeExtra = writeExtra
		self._readQuorumSplits = []
		for q in self._readQuorums:
			c = 0
			for r in q:
				c += splitsPerReplica[r]
			self._readQuorumSplits.append(c)
		self._writeQuorumSplits = []
		for q in self._writeQuorums:
			c = 0
			for r in q:
				c += splitsPerReplica[r]
			self._writeQuorumSplits.append(c)

	def readLatency(self, replicaNames, replicaLatencies):
		qlats = []
		for _ in self._readQuorums:
			qlats.append([])
		for i in range(len(replicaNames)):
			n = replicaNames[i]
			for j in range(len(self._readQuorums)):
				if n in self._readQuorums[j]:
					qlats[j].append(replicaLatencies[j])
		minLat = float("inf")
		for i in range(len(qlats)):
			assert len(qlats[i]) == self._readQuorumSplits[i]
			qlats[i].sort()
			minLat = min(minLat, qlats[i][-1-self._readExtra])
		return minLat

	def writeLatency(self, replicaNames, replicaLatencies):
		qlats = []
		for _ in self._writeQuorums:
			qlats.append([])
		for i in range(len(replicaNames)):
			n = replicaNames[i]
			for j in range(len(self._writeQuorums)):
				if n in self._writeQuorums[j]:
					qlats[j].append(replicaLatencies[j])
		minLat = float("inf")
		for i in range(len(qlats)):
			assert len(qlats[i]) == self._writeQuorumSplits[i]
			qlats[i].sort()
			minLat = min(minLat, qlats[i][-1-self._writeExtra])
		return minLat

	def useReqPerSplit(self):
		return True

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
				latencies[dc1][dc2].append(l*1e3)
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

def GetRequestGenerator(networkLatencies, readStorageLatencies, randomFrontEnd, listOfReplicas, quorumSystem, numberOfSplits):
	replicaNames = []
	replicaLatencies = []
	if quorumSystem.useReqPerSplit():
		for dc in listOfReplicas:
			for _ in range(numberOfSplits(dc)):
				networkLatency = 0
				if dc != randomFrontEnd:
					networkLatency = random.choice(networkLatencies[randomFrontEnd][dc])
				storageLatency = random.choice(readStorageLatencies[dc])
				replicaNames.append(dc)
				replicaLatencies.append(networkLatency + storageLatency)
	else:
		for dc in listOfReplicas:
			networkLatency = 0
			if dc != randomFrontEnd:
				networkLatency = random.choice(networkLatencies[randomFrontEnd][dc])
			storageLatency = random.choice(readStorageLatencies[dc])
			replicaNames.append(dc)
			replicaLatencies.append(networkLatency + storageLatency)

	return quorumSystem.readLatency(replicaNames, replicaLatencies)

def PutRequestGenerator(networkLatencies, storageLatencies, randomFrontEnd, listOfReplicas, quorumSystem, numberOfSplits, useFlexiblePaxos):
	latency = 0
	if useFlexiblePaxos:
		replicaNames = []
		replicaLatencies = []
		if quorumSystem.useReqPerSplit():
			for dc in listOfReplicas:
				for _ in range(numberOfSplits(dc)):
					networkLatency = 0
					if dc != randomFrontEnd:
						networkLatency = random.choice(networkLatencies[randomFrontEnd][dc])
					storageLatency = random.choice(storageLatencies[0][dc])
					replicaNames.append(dc)
					replicaLatencies.append(networkLatency + storageLatency)
		else:
			for dc in listOfReplicas:
				networkLatency = 0
				if dc != randomFrontEnd:
					networkLatency = random.choice(networkLatencies[randomFrontEnd][dc])
				storageLatency = random.choice(storageLatencies[0][dc])
				replicaNames.append(dc)
				replicaLatencies.append(networkLatency + storageLatency)

		latency += quorumSystem.readLatency(replicaNames, replicaLatencies)
	else:
		replicaNames = []
		replicaLatencies = []
		if quorumSystem.useReqPerSplit():
			for dc in listOfReplicas:
				for _ in range(numberOfSplits(dc)):
					networkLatency = 0
					if dc != randomFrontEnd:
						networkLatency = random.choice(networkLatencies[randomFrontEnd][dc])
					storageLatency = random.choice(storageLatencies[0][dc])
					replicaNames.append(dc)
					replicaLatencies.append(networkLatency + storageLatency)
		else:
			for dc in listOfReplicas:
				networkLatency = 0
				if dc != randomFrontEnd:
					networkLatency = random.choice(networkLatencies[randomFrontEnd][dc])
				storageLatency = random.choice(storageLatencies[0][dc])
				replicaNames.append(dc)
				replicaLatencies.append(networkLatency + storageLatency)

		latency += quorumSystem.writeLatency(replicaNames, replicaLatencies)


	replicaNames = []
	replicaLatencies = []
	if quorumSystem.useReqPerSplit():
		for dc in listOfReplicas:
			for _ in range(numberOfSplits(dc)):
				networkLatency = 0
				if dc != randomFrontEnd:
					networkLatency = random.choice(networkLatencies[randomFrontEnd][dc])
				storageLatency = random.choice(storageLatencies[1][dc])
				replicaNames.append(dc)
				replicaLatencies.append(networkLatency + storageLatency)
	else:
		for dc in listOfReplicas:
			networkLatency = 0
			if dc != randomFrontEnd:
				networkLatency = random.choice(networkLatencies[randomFrontEnd][dc])
			storageLatency = random.choice(storageLatencies[1][dc])
			replicaNames.append(dc)
			replicaLatencies.append(networkLatency + storageLatency)

	latency += quorumSystem.writeLatency(replicaNames, replicaLatencies)

	return latency

def main():
	# make execution deterministic
	random.seed(0)
	parser = argparse.ArgumentParser()
	parser.add_argument("--op-count", default=50000, type=int)
	parser.add_argument("configpath")
	parser.add_argument("outputpath")
	args = parser.parse_args()

	FLEXIBLE_PAXOS = None
	configFile = args.configpath
	numberOfRequests = args.op_count

	if "fixautosplit" in configFile or "splitsasym-auto" in configFile:
		quorumSystem = "basic"
		FLEXIBLE_PAXOS = False
	elif "flexbasic" in configFile:
		quorumSystem = "basic"
		FLEXIBLE_PAXOS = True
	elif "flexgen" in configFile:
		quorumSystem = "spec"
		FLEXIBLE_PAXOS = True
	elif "flexgenexp" in configFile:
		quorumSystem = "specrps"
		FLEXIBLE_PAXOS = True
	else:
		print "Error: Invalid nomenclature for the config file name"
		return

	networkLatencies = ReadNetworkLatencies()
	storageLatencies = ReadStorageLatencies()
	readQuorumSize = 0
	writeQuorumSize = 0
	accessSet = []
	listOfReplicas = []
	numberOfSplits = {}
	specificReadQuorums = [None] * 46 #Since 46 DCs
	specificWriteQuorums = [None] * 46 #Same reason

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

	print "Read quorum size", readQuorumSize
	print "Write quorum size", writeQuorumSize
	print "Access set", accessSet
	print "List of replicas", listOfReplicas
	print "Flexible paxos enabled", FLEXIBLE_PAXOS
	print "read quorums", specificReadQuorums
	print "write quorums", specificWriteQuorums

	if not os.path.exists(os.path.dirname(args.outputpath)):
		try:
			os.makedirs(os.path.dirname(args.outputpath))
		except OSError as exc: # Guard against race condition
			if exc.errno != errno.EEXIST:
				raise
	outputFile = open(args.outputpath, 'w')

	if quorumSystem == "basic":
		print "Runnning siumlation for basic quorum system..."
		quorumSystem = BasicQuorumSystem(readQuorumSize, writeQuorumSize)
		for _ in range(numberOfRequests):
			frontend = random.choice(accessSet)
			sampleGet = GetRequestGenerator(networkLatencies, storageLatencies[0], frontend, listOfReplicas, quorumSystem, {})
			samplePut = PutRequestGenerator(networkLatencies, storageLatencies, frontend, listOfReplicas, quorumSystem, {}, FLEXIBLE_PAXOS)
			outputFile.write("get " + str(sampleGet) + " put " + str(samplePut) + "\n")

	elif quorumSystem == "spec":
		print "Running simmulation for specific quorum system..."
		quorumSystem = SpecifiedQuorumSystem(specificReadQuorums, specificWriteQuorums)
		for _ in range(numberOfRequests):
			frontend = random.choice(accessSet)
			sampleGet = GetRequestGenerator(networkLatencies, storageLatencies[0], frontend, listOfReplicas, quorumSystem, numberOfSplits)
			samplePut = PutRequestGenerator(networkLatencies, storageLatencies, frontend, listOfReplicas, quorumSystem, numberOfSplits, FLEXIBLE_PAXOS)
			outputFile.write("get " + str(sampleGet) + " put " + str(samplePut) + "\n")

	elif quorumSystem == "specrps":
		print "Running simmulation for specific RPS quorum system..."
		quorumSystem = SpecifiedRPSQuorumSystem(numberOfSplits, specificReadQuorums, specificWriteQuorums, 1, 1)
		for _ in range(numberOfRequests):
			frontend = random.choice(accessSet)
			sampleGet = GetRequestGenerator(networkLatencies, storageLatencies[0], frontend, listOfReplicas, quorumSystem, numberOfSplits)
			samplePut = PutRequestGenerator(networkLatencies, storageLatencies, frontend, listOfReplicas, quorumSystem, numberOfSplits, FLEXIBLE_PAXOS)
			outputFile.write("get " + str(sampleGet) + " put " + str(samplePut) + "\n")



if __name__ == '__main__':
	main()
