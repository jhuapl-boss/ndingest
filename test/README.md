# Tests

* Run py.test to test functions
```console
py.test
```
* lambda functions are symlinked here for now. This is because emulambda wants it to be pointed as a module.Will be fixed later

## Tests Relevant to the Boss

```shell
# Use randomized queue names.
export NDINGEST_TEST=1
pytest -c test_apl.cfg
```

When testing for the Boss, these tests should be run:
* test_bossingestproj.py
* test_bosssettings.py
* test_bosstileindexdb.py
* test_bossutil.py
* test_cleanupqueue.py
* test_deadletter_queue.py
* test_tilebucket.py
* test_uploadqueue.py

