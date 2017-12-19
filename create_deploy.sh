#! /bin/bash

mkdir dist
echo "Copying local files to dist/.."
cp -rf src data dist
echo "Copying dependencies to dist/.."
cp -rf $VIRTUAL_ENV/lib/python3.6/site-packages/* dist 1> /dev/null
echo "Deleting pip, *egg-info, __pycache__ dirs"
# TODO don't copy in the first place?
rm -rf dist/pip
find dist -path '*/*egg-info/*' -delete
find dist -type d -name '*egg-info' -delete
find dist -path '*/__pycache__/*' -delete
find dist -type d -name '__pycache__' -delete
echo "Zipping dist/ to deployment.zip.."
cd dist/; zip -r deployment.zip . 1> /dev/null
cd ..
mv dist/deployment.zip .
echo "Removing dist/.."
rm -rf dist/
