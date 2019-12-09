python_library(
  name = 'test-lib',
  dependencies = [
    'lambda/src/python/paytm/job'
  ],
  sources = ['stub_factory.py']
)

python_tests(
  dependencies = [
    'lambda/src/python/paytm/job',
    ':test-lib'
  ]
)
