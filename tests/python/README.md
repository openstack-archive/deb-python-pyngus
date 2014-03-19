# Unit Tests #

The unit test are run using the `tox` command from the [tox automation
project](https://testrun.org/tox/latest) for more detail.  It is
assumed tox is available on your system.

To run the tests:

1. The Proton Python binding must be installed [1]
2. From the topmost directory (that contains the tox.ini file) execute the *tox* command
3. That's it!

The unit tests may be run without tox, but your environment must be
set up so that the module is able to be imported by the test scripts.
For example, setting the PYTHONPATH environment variable to include
the path to the build directory.

[1] You can still run the tests without installing Proton.  You can
run the tests using a build of the Proton sources instead.  In order
to do this you **MUST** source the config.sh file supplied in the
Proton sources prior to running the unit tests.  This script adds the
Proton Python bindings to the search path used by the unit tests.
