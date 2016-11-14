# Tests

* Run py.test to test functions
```console
pytest
```
* lambda functions are symlinked here for now. This is because emulambda wants it to be pointed as a module.Will be fixed later

## Tests Relevant to the Boss

Only the following tests should be run when working with the Boss.
`test_apl.cfg` configures pytest to run only the tests that matter to the Boss.

```shell
# Use randomized queue names.
export NDINGEST_TEST=1

# From the ndingest root folder:
pytest -c test_apl.cfg
```

When testing for the Boss, only these tests apply:
* test_bossingestproj.py
* test_bosssettings.py
* test_bosstileindexdb.py
* test_bossutil.py
* test_cleanupqueue.py
* test_deadletter_queue.py
* test_tilebucket.py
* test_uploadqueue.py

