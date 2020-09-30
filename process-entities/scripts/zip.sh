#!/bin/bash

rm deployment.zip
rm -rf node_modules
NODE_ENV=production yarn
zip -r deployment.zip .
