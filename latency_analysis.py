#!/usr/bin/env python

import os
import sys 
import time
import ast

DATA_PATH_PART1 = "/d01/old_vault_backup/zwu/dns_measurement_data.2/between_cloud_measurement"
DATA_PATH_PART2 = "/d01/old_vault_backup/zwu/dns_measurement_data.3/between_cloud_measurement"


networklatencies = {}

def ReadNetworkLatencies(path):
    # global networklatencies
    dataCenters = os.listdir(path)
    for dc in dataCenters:
      if "azure" in dc:
          print "Processing data center",dc,"within path", path
          if dc not in networklatencies:
            networklatencies[dc] = {}

          dates = os.listdir(path + "/" + dc)
          for date in dates:
            try:
                dataFile = open(path + "/" + dc + "/" + date + "/log_time_no_stop")
            except IOError as io:
                print io
                print "Missing data for dc", dc
            lines = dataFile.readlines()
            for line in lines:
                lineData = line.strip().split("||")
                dc2 = lineData[1]
                if dc2 not in networklatencies[dc] and "azure" in dc2:
                    networklatencies[dc][dc2] = {}
                month =  int(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(lineData[3]))).split('-')[1])
                if "azure" in dc2 and month not in networklatencies[dc][dc2]:
                    networklatencies[dc][dc2][month] = []
                if lineData[2] == "site" and lineData[4] != "ERROR" and lineData[5] != "site" and "azure" in dc2:
                    # print lineData
                    try:
                        # if dc == "azure_eastus" and dc2 == "azure_centralus" and "dns_measurement_data.3" in dataFile.name:
                            # print "appending the value ", float(lineData[7])-float(lineData[6]), "as the line data is ", lineData, "and the filename is", dataFile.name
                            # print len(networklatencies["azure_eastus"]["azure_centralus"][9])
                        networklatencies[dc][dc2][month].append(float(lineData[7])-float(lineData[6]))
                    except ValueError as v:
                        print lineData, v

def median(lst):
    sortedLst = sorted(lst)
    lstLen = len(lst)
    if not lstLen:
        return 0
    index = (lstLen - 1) // 2

    return sortedLst[index]


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == "write":
        ReadNetworkLatencies(DATA_PATH_PART1)
        # print "Done with parsing with part1", len(networklatencies["azure_eastus"]["azure_centralus"][9])
        ReadNetworkLatencies(DATA_PATH_PART2)
        # print "Done with parsing with part2", len(networklatencies["azure_eastus"]["azure_centralus"][9])
        outputFile = open("./NetworkLatenciesDict","w")
        outputFile.write(str(networklatencies))

    inputFile = open("./NetworkLatenciesDict",'r').read()
    networklatenciesRead = eval(inputFile)
    # Print the latencies for each pair for each month for further analysis
    # print "Done processing the network latency file", len(networklatenciesRead.keys())
    for dc1 in networklatenciesRead:
        for dc2 in networklatenciesRead[dc1]:
            for month in range(1,13):
                networklatenciesRead[dc1][dc2][month] = median(networklatenciesRead[dc1][dc2][month])
                if not networklatenciesRead[dc1][dc2][month]:
                    print "No value for the entire month of", dc1, dc2, month
                # print dc1, dc2, month, networklatenciesRead[dc1][dc2][month]

    # Analysis the latencies per data center pairs

    stability = []
    stepsize = [1]
    # networklatenciesRead = networklatencies
    for steps in stepsize:
        for dc1 in networklatenciesRead:
            for dc2 in networklatenciesRead[dc1]:
                maxFlucatuation = 0
                baseMonth = 0
                for month in range(1,12):
                    baseMonth += 1
                    if baseMonth + steps <= 6 and dc1 != dc2 and baseMonth != 6:
                        print "Computing fluctuation from", networklatenciesRead[dc1][dc2][baseMonth] , networklatenciesRead[dc1][dc2][baseMonth + steps],abs(networklatenciesRead[dc1][dc2][baseMonth] - networklatenciesRead[dc1][dc2][baseMonth + steps])
                        flucatutation = (abs(networklatenciesRead[dc1][dc2][baseMonth]-networklatenciesRead[dc1][dc2][baseMonth+steps]))/float(networklatenciesRead[dc1][dc2][baseMonth])
                        if flucatutation > maxFlucatuation:
                            maxFlucatuation = flucatutation
                print "Maximum fluctuation inlatencies for ", dc1, dc2, "is ", maxFlucatuation
                stability.append(maxFlucatuation)
        print "For step size", steps, "maximum fluctuation in latency across data centers ", stability, max(stability)



