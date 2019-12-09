import os
import sys
import argparse

def runOSCommand(cmd):
    print(cmd)
    sys.stdout.flush()
    rc = os.system(cmd)
    if rc > 0:
        raise Exception('Command Failed!!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('baseResDir', help='basedir where featurelist_india_new_csv is located')
    parser.add_argument('resourceDir', help='upload directory of s3')
    parser.add_argument('flow', help='Flow which is being run. Options: merchant|customer')

    args = parser.parse_args()

    flow = args.flow
    featListFileName = ""
    specSheetPath = ""
    if flow == 'merchant':
        featListFileName = "/feature_list_india_merchant.csv"
        specSheetPath = "/SpecSheetsMerchant/"
    elif flow == 'customer':
        featListFileName = "/feature_list_india_new.csv"
        specSheetPath = "/SpecSheets/"
    elif flow == 'channel':
        featListFileName = "/feature_list_india_channel.csv"
        specSheetPath = "/SpecSheets/"
    else:
        raise Exception('Invalid flow name')
    ## Make Paths
    # Local
    specsPathLocal = args.baseResDir + specSheetPath
    featListPathLocal = args.baseResDir + featListFileName
    #S3 Paths
    specsPathS3 = args.resourceDir + specSheetPath
    featListPathS3 = args.resourceDir + featListFileName

    ##Command for the FeatureList
    featListCommand = 'aws s3 --region "ap-south-1" cp %s %s' % (featListPathLocal,featListPathS3)
    runOSCommand(featListCommand)

    ##Command for the FeatureList
    #Clear Earlier Specs.
    delCommand = 'aws s3 --region "ap-south-1" rm --recursive %s' % (specsPathS3)
    runOSCommand(delCommand)
    # Upload new
    specsCommand = 'aws s3 --region "ap-south-1" cp --recursive %s %s' % (specsPathLocal,specsPathS3)
    runOSCommand(specsCommand)

