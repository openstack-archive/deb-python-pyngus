# Pyngus #

[![Build Status](https://travis-ci.org/kgiusti/pyngus.svg)](https://travis-ci.org/kgiusti/pyngus)

A messaging framework built on the QPID Proton engine.  It provides a
callback-based API for message passing.

See the User Guide in the docs directory for more detail.

## Release 2.1.4 ##

* avoid using deprecated next_tick in the container
* enable Python 3.5 testing in tox
* add client authentication via SSL tests
* bump max proton version to 0.16

## Release 2.1.3 ##

* Remove chatty debug log messages
* fix pep8 violation
* add static performace test tool
* Bump max proton version to 0.15

## Release 2.1.2 ##

* Bump max proton version to 0.14

## Release 2.1.1 ##

* bugfix: under some (rare) flow/credit interactions a sender may
  stall.  Changed code to invoke credit_granted() callback more
  frequently.

## Release 2.1.0 ##

* feature: add 'x-force-sasl' to connection property map
* bugfix: update old SASL unit test

## Release 2.0.4 ##

* Bump max proton version to 0.13
* performance tweak to link event handling
* fix perf-test.py tool
* bugfix: fix leak of timer callbacks
* enable Python 3.4 testing
* bugfix: fix receiver example (recv.py)
* several fixes to the SASL unit tests
* bugfix: fix leak of underlying proton objects
* Add SASL/SSL configuration options to examples
* bugfix: allow PLAIN or ANONYMOUS authentication in server mode

## Release 2.0.3 ##

* bugfix: fixed a memory leak
* bugfix: cyrus test fixed

## Release 2.0.0 ##

* Support for proton 0.10
* The SASL API has changed due to an API change in proton 0.10
  * Proton 0.10 implements SASL via the Cyrus SASL library
    * this change allows use of more secure authentication mechanisms, such as Kerberos
  * Applications should no longer directly access the Proton SASL class via the Connection.pn\_sasl property
    * instead, the following new properties may be passed to the Container.create\_connection() method:
      * x-username - (client only) the authentication id
      * x-password - (client only) the authentication password
      * x-require-auth - (server only) reject clients that do not use authentication
      * x-sasl-mechs - (server only) whitespace delimited string of
        acceptable mechanisms.  If not supplied the mechanisms
        specified in the system's SASL configuration will be used.
        This option should only be used when the application wants to
        further restrict the set of acceptable mechanisms.
      * x-sasl-config-dir - (server only) the location of the
        _directory_ that holds the system's Cyrus SASL configuration.
      * x-sasl-config-name - (server only) the name of the SASL
        configuration file (*without* the ".conf" suffix) in the
        x-sasl-dir directory.
      * *NOTE WELL*: Cyrus SASL cannot support multiple different SASL
        configurations per connection.  The values of
        x-sasl-config-dir and x-sasl-config-name *MUST* be the same
        for all connections that use SASL.
  * the ConnectionEventHandler.sasl\_step() callback has been deprecated as proton 0.10 no longer uses it
    * The ConnectionEventHandler.sasl\_done() callback *is* still supported.
* Pyngus now enforces strict reentrancy checking.  Attempting to call
  a non-reentrant Pyngus method will now throw a RuntimeError exception.

## Release 1.3.0 ##

* Support for proton 0.9
* Installation of proton dependencies via setup.py.  This feature was
  added by Flavio Percoco Premoli <flaper87@gmail.com> - thanks
  Flavio!
