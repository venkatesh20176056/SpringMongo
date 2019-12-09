import csv
inputPath="/tmp/output.csv"
featuresToKeepPath = "/Users/amritpraharaj/Documents/featuresToKeepUpdated.csv"

r1=csv.reader(open(inputPath))
lines1=list(r1)

allFeatures = set()
allEnabledFeatures = set()

for line in lines1:
     allFeatures.add(line[3])
     if(line[7]=="1"):
           allEnabledFeatures.add(line[3])

r2=csv.reader(open(featuresToKeepPath))
lines2=list(r2)

for line in lines2:
    feature=line[0]
    if feature not in allFeatures:
        print "[MISSING] "+feature # print missing features
    elif feature in allFeatures and feature not in allEnabledFeatures:
        print "[isEnable=0] "+feature # print features whose isEnable is 0





