# LookAlike Lambda

## Setup Pants
Run `./pants` from the repo root directory. The first time pants-build runs, it will bootstrap
itself, which takes a couple of minutes. Pants-build does not change your Python installations
in any way.

## Test and Type-check
./pants test mypy lambda/:: --tag=-integration -- --ignore-missing-imports

## Run Integration Tests
Start DynamoDb locally: docker run -p 8000:8000 amazon/dynamodb-local
Run DynamoDb tests: ./pants test mypy lambda/:: --tag=+dynamodb -- --ignore-missing-imports

## Repl
./pants repl lambda/src/python/paytm/look_alike:look-alike-lambda

## Run locally
./pants test mypy lambda/:: --tag=-integration -- --ignore-missing-imports && ./pants run lambda/src/python/paytm/look_alike:look-alike-lambda

## Package
./pants test mypy lambda/:: --tag=-integration -- --ignore-missing-imports && ./pants bundle lambda/src/python/paytm/look_alike:look-alike-lambda

## Package as AWS Lambda
Install a tool to convert pex files to lambda-runnable format: `pip3 install lambdex`
./pants test mypy lambda/:: --tag=-integration -- --ignore-missing-imports && \
    ./pants bundle lambda/src/python/paytm/look_alike:look-alike-lambda && \
    lambdex build -e paytm.look_alike.look_alike_lambda:lambda_handler dist/look-alike-lambda.pex && \
    mv dist/look-alike-lambda.pex dist/look-alike-lambda.zip
