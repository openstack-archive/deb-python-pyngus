# Unit Tests #

The unit test are run using the `tox` command from the [tox automation
project](https://testrun.org/tox/latest) for more detail.  It is
assumed tox is available on your system.

To run the tests:

1. The Proton Python binding must be installed [1]
2. From the topmost directory (that contains the tox.ini file) execute the *tox* command
3. That's it!

The unit tests may be run without tox, but your environment must be
set up so that the dingus module is able to be imported by the test
scripts.  For example, setting the PYTHONPATH environment variable to
include the path to the build directory.

[1] You can still run the tests without installing Proton.  You can
run the tests using a build of the Proton sources instead.  In order
to do this you **MUST** source the config.sh file supplied in the
Proton sources prior to running the unit tests.  This script adds the
Proton Python bindings to the search path used by the unit tests.


# Performance Test #

perf-test.py is a static performance test.  It simulates sending many
small messages over many links.  It can be used to gauge the
performance impact of code changes during development.

To run, invoke it using the *time* command.  Example:

    $ time ./tests/python/perf-test.py
    
    real	2m15.789s
    user	2m15.357s
    sys 	0m0.039s

## Historical Results ##

### Lenovo T530 ###

    master @ 3c0f144eceed8b8b0a16d9947ef66cbfef82f95e  (Proton 0.7 RC3)
    real	2m15.789s
    user	2m15.357s
    sys 	0m0.039s

