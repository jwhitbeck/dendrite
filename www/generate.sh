#!/bin/bash

# Small script to fully regenerate the website

set -e

# clear everything under public
rm -rf public/*

# Renegerate static assets
uglifyjs --compress --mangle -- res/site.js > static/js/site.min.js

# Regenerate docs
pushd .
cd ../dendrite/
lein doc
popd

# Regenerate static website
hugo
