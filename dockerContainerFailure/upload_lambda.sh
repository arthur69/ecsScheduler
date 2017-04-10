#!/bin/bash

# use parent directory name; should be same as metric name
lambda_name=`basename $1`

function clean () {
	find . -name "*.pyc" -exec rm -rf {} \;
	rm *.zip
	# echos are captured in return values
	echo "$(ls)"
}

function download_dependencies() {
    echo "Downloading dependencies..."
    pip install -r requirements.txt -t .
}

function compress () {
	echo "Compressing everything..."
	zip -r $lambda_name.zip *
}

function upload () {
    echo "Uploading to AWS..."
	aws lambda update-function-code \
	--function-name "${lambda_name}" \
	--zip-file fileb://$PWD/$lambda_name.zip
}

## MAIN ##
cd $lambda_name
preserve=$(clean)
download_dependencies
compress
upload

# Delete unecessary files
new="$(ls)"
deleteThis=`diff <(echo "$preserve") <(echo "$new") | grep -v "^---" | grep -v "^[0-9c0-9]" | sed 's/>//' | sed 's/<//'`
echo "Removing unecessary dir/files:" $deleteThis
rm -r $deleteThis