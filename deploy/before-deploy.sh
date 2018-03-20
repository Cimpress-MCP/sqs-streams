#!/bin/bash
if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    openssl aes-256-cbc -K $encrypted_69ce32170be3_key -iv $encrypted_69ce32170be3_iv -in deploy/signingkey.asc.enc -out deploy/signingkey.asc -d
    gpg --fast-import deploy/signingkey.asc
fi