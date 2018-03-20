#!/bin/bash
if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    openssl aes-256-cbc -K $encrypted_69ce32170be3_key -iv $encrypted_69ce32170be3_iv -in deploy/codesigning.asc.enc -out deploy/codesigning.asc -d
    gpg --fast-import deploy/codesigning.asc
    mvn build-helper:parse-version versions:set -DnewVersion=\${parsedVersion.majorVersion}.\${parsedVersion.minorVersion}.${TRAVIS_BUILD_NUMBER}-${TRAVIS_COMMIT:0:7}
    mvn deploy -P sign,build-extras --settings deploy/mvnsettings.xml
fi