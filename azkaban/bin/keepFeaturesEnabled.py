import csv

inputPath = "/Users/amritpraharaj/Documents/map-features/azkaban/resources/feature_list_india_new.csv"
featuresToKeepPath = "/Users/amritpraharaj/Documents/featuresToKeepUpdated.csv"
derivedFeaturesToKeepPath = "/Users/amritpraharaj/Documents/derivedFeaturesToKeep.csv"
outputPath="/tmp/output.csv"

r=csv.reader(open(inputPath))
lines=list(r)

with open(featuresToKeepPath) as f:
    content = f.readlines()

featuresToKeep = set(x.strip() for x in content)

with open(derivedFeaturesToKeepPath) as f:
    content = f.readlines()

derivedFeaturesToKeep = set(x.strip() for x in content)
count=0

for line in lines:
    if(count==0):
        count=count+1
        continue # Escaping header line
    if(line[1]=="BACKWARD-COMPATIBILITY"):
        continue # Escaping backward compatibility ones
    feature=line[3]
    if(line[8]=="NESTED"):
        feature=feature.split(".")[0]
    if feature not in featuresToKeep:
       line[9]=0
       line[7]=0
    if feature in derivedFeaturesToKeep:
        line[8]=line[8].strip().replace("DERIVED","EXTRA")

#Only Add the below line once
missingFeatureFromUsedFeatures = ["Transactional","Payments","ONLINE_ALL","ONLINE_total_transaction_count_1_days","total transaction count 1 days","Long","ONLINE total transaction count 1 days","1","ONLINE","1"]
lines.append(missingFeatureFromUsedFeatures)

writer = csv.writer(open(outputPath, 'w'))
writer.writerows(lines)







