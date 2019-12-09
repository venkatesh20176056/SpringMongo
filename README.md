# MAP Features

The MAP features consists of 3 main components:

* jobs that create features to be used by the marketing advertising platform (MAP)
* a pipeline to export snapshots of the features to other serivces in the MAP
* a feature REST service to expose the feature metadata

```
To assemble the fat-jar:
- sbt "project mapfeatures" assembly
```

To create the binary for azkaban:
- sbt "project mapfeatures" "packageAzkabanBin staging|prod"

To create the binary for airflow:
- sbt "project mapfeatures" "packageAirflowBin staging|prod"

To create the airflow dags:
- sbt "project mapfeatures" "packageAirflow staging|prod"

To create the raw flow:
- sbt "project mapfeatures" "packageRawFlow staging|prod"

To create the snapshot flow:
- sbt "project mapfeatures" "packageSnapshotFlow staging|prod"

To create the export flow:
- sbt "project mapfeatures" "packageExportFlow staging|prod"

## Doing a release

> Create release branch
```
> Merge all code to develop
git checkout develop
git pull
git checkout -b release/<release_number>
```
> Bump version number in Build.scala
```
> edit project/Build.scala to bump version
git add project/Build.scala
git commit -m "version bump"
git push -u origin release/<release_number>
```
> Merge into master
```
git checkout master
git pull
git merge --no-ff release/<release_number>
```
> Create release tag
```
git tag -a v<release_number>
```
> Merge the tag into develop
```
git checkout develop
git pull
git merge --no-ff v<release_number>
```
> Push everything to remote
```
git push origin master
git push origin develop
git push origin --tags
git push origin release/<release_number>
```

## Deploying to staging
```
- merge release branch to develop
- build project for staging run:
    > https://jenkins.paytmlabs.com/view/map-features-staging/job/map-features-build-staging/
- deploy code to staging:
    > https://jenkins.paytmlabs.com/view/map-features-staging/job/map-features-deploy-base-staging/
```


## Deploying to prod
```
- merge release branch to master
- build project for production run:
    > https://jenkins.paytmlabs.com/view/map-features-prod/job/map-features-build-prod/
- deploy code to production:
    > https://jenkins.paytmlabs.com/view/map-features-prod/job/map-features-deploy-base-prod/
```
