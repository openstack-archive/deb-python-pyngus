#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#####
# An example script that creates certificates that can be used for
# testing SSL.  Uses the openssl and keytool commands to:
#
# * Create a CA certificate that can be used to verify the peer's
# certificate.
#
# * Create a server certificate signed by the CA, and a private key
# file. The password for the private key file is 'server-password'
#
# * Create a client certificate signed by the CA, and a private key
# file.  The password for the private key file is 'client-password'.
# This certificate can be used for authentication of a client by the
# server.
#
# openssl is provided by the OpenSSL project.
# keytool is provided by the Java JDK
###

#set -x

OPENSSL=$(type -p openssl)
if [[ !(-x $OPENSSL) ]] ; then
    echo >&2 "'openssl' command not available, certificates not generated"
    exit 0
fi
KEYTOOL=$(type -p keytool)
if [[ !(-x $KEYTOOL) ]] ; then
    echo >&2 "'keytool' command not available, certificates not generated"
    exit 0
fi

# clean up old stuff
rm -f *.pem *.pkcs12

CA_COMMON_NAME="example.ca.com"
# Create a self-signed certificate for the CA, and a private key to sign certificate requests:
$KEYTOOL -storetype pkcs12 -keystore ca.pkcs12 -storepass ca-password -alias ca -keypass ca-password -genkey -dname "CN=$CA_COMMON_NAME" -validity 9999
$OPENSSL pkcs12 -nokeys -passin pass:ca-password -in ca.pkcs12 -passout pass:ca-password -out ca-certificate.pem

SERVER_COMMON_NAME="*.server.com"
# Create a certificate request for the server certificate.  Use the CA's certificate to sign it:
$KEYTOOL -storetype pkcs12 -keystore server.pkcs12 -storepass server-password -alias server-certificate -keypass server-password -genkey  -dname "CN=$SERVER_COMMON_NAME" -validity 9999
$KEYTOOL -storetype pkcs12 -keystore server.pkcs12 -storepass server-password -alias server-certificate -keypass server-password -certreq -file server-request.pem
$KEYTOOL -storetype pkcs12 -keystore ca.pkcs12 -storepass ca-password -alias ca -keypass ca-password -gencert -rfc -validity 99999 -infile server-request.pem -outfile server-certificate.pem
$OPENSSL pkcs12 -nocerts -passin pass:server-password -in server.pkcs12 -passout pass:server-password -out server-private-key.pem

CLIENT_COMMON_NAME="my.client.com"
# Create a certificate request for the client certificate.  Use the CA's certificate to sign it:
$KEYTOOL -storetype pkcs12 -keystore client.pkcs12 -storepass client-password -alias client-certificate -keypass client-password -genkey  -dname "CN=$CLIENT_COMMON_NAME" -validity 99999
keytool -storetype pkcs12 -keystore client.pkcs12 -storepass client-password -alias client-certificate -keypass client-password -certreq -file client-request.pem
keytool -storetype pkcs12 -keystore ca.pkcs12 -storepass ca-password -alias ca -keypass ca-password -gencert -rfc -validity 99999 -infile client-request.pem -outfile client-certificate.pem
openssl pkcs12 -nocerts -passin pass:client-password -in client.pkcs12 -passout pass:client-password -out client-private-key.pem

# clean up all the unnecessary stuff
rm *.pkcs12 *-request.pem
