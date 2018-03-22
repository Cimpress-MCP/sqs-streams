#!/bin/bash

openssl aes-256-cbc -K $encrypted_69ce32170be3_key -iv $encrypted_69ce32170be3_iv -in deploy/codesigning.asc.enc -out deploy/codesigning.asc -d
gpg --fast-import deploy/codesigning.asc
mvn deploy -P sign --settings deploy/mvnsettings.xml