
import os
import sys

basePathStg = "s3://midgar-aws-workspace/stg/mapfeatures/features"
basePathProd = "s3://midgar-aws-workspace/prod/mapfeatures/features"


def executeCommand(cmd):
    sys.stdout.flush()
    rc = os.system(cmd)
    if rc > 0:
        raise Exception('Command failed:',cmd)


def copyData(key, srcDate, targetDate):
    command = 'aws s3 cp --recursive --region "ap-south-1" %s/%s/dt=%s/ %s/%s/dt=%s/' % (
        basePathProd, key, srcDate,
        basePathStg, key, targetDate
    )
    print(command)
    executeCommand(command)

def forAll(keyList, srcDate, targetDate):
    for aKey in keyList:
        copyData(aKey, srcDate, targetDate)


def getKeyList(groups, keys):
    lst = []
    for aGrp in groups:
        idKey = "%d/ids" % (aGrp)
        lst.append(idKey)
        for akey in keys:
            finalKey = "%d/levels/%s" % (aGrp, akey)
            lst.append(finalKey)
    return lst


if __name__ == "__main__":
    srcDate = "2018-06-17"
    targetDate = "2018-06-18"
    groups = [1,]
    keys = ["BK", "EC", "GABK", "GAL2BK", "GAL2EC", "GAL2RU", "GAL3BK", "GAL3EC1", "GAL3EC2", "GAL3RU", "GAPAYTM",
            "L3BK", "L3EC1", "L3EC2", "L3RU", "PAYTM", "RU", "payments", "wallet", "bank"]
    allKeys = ["profile", ] + getKeyList(groups, keys)
    forAll(allKeys,srcDate,targetDate)
