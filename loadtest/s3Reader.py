import boto3
import botocore
import requests

globalFilename = None
globalFirstLine = None
globalResponseCode = None
globalBody = ''
globalResponseNormal = False


def printFileResponse():
    global globalFilename
    global globalFirstLine
    global globalResponseCode
    global globalBody
    global globalResponseNormal

    if (globalResponseNormal):
        return

    if (globalFilename is not None):
        print "file=" + globalFilename
        globalFilename = None

    if (globalFirstLine is not None):
        print globalFirstLine
        globalFirstLine = None

    if (not globalResponseNormal):
        if (globalResponseCode is not None):
            print globalResponseCode
            globalResponseCode = None

    if (globalBody is not ''):
        print globalBody
        globalBody = ''

def printList(allObjs):
    for obj in allObjs:
        print('obj ', obj)
        key = obj.key
        body = obj.get()['Body'].read()

def listAllObjects(bucket, listFilter):
# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
    allObjs = list(bucket.objects.filter(Prefix=listFilter))
#    printList(allObjs)
    return allObjs

def getFiles(bucket, environment, lastTimestamp):
    prefix = environment + '/' + lastTimestamp
    allFiles = list(bucket.objects.filter(Prefix=prefix))
#    printList(allFiles)
    return allFiles


def readFile(bucket, environment, lastTimestamp, filename):
#    print 'file=' + filename
    try:
        obj = bucket.Object(filename)
        body = obj.get()['Body'].read()
#    print 'body='
#    print body
        return body
    except botocore.exceptions.EndpointConnectionError as endpoint:
        print "Problem with readfile " + str(endpoint)
        return None

def setFirstLine(filename, body):
#    print "file=" + filename
    global globalFilename
    global globalFirstLine

    globalFilename = filename
    try:
        firstLine = body.splitlines()[0]
#        print firstLine
        globalFirstLine = firstLine
        return firstLine
    except IndexError:
#        print "body was empty"
        globalFirstLine = "body was empty"
        return None


def allObjects(bucket):
# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
    for obj in bucket.objects.all():
        print('obj ', obj)
        key = obj.key
        body = obj.get()['Body'].read()

def ignoreMe():

    bucketName = 'edmunds-perftesting-testplans'
    key = ''

    try:
        s3.Bucket(bucketName).download_file(KEY, 'my_local_image.jpg')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

def getAllTimestamps(allObjects):
    allTimestamps = []
    for obj in allObjects:
        splitter = obj.key.split('/')
        timestamp = splitter[1]
        if timestamp not in allTimestamps:
            allTimestamps.append(timestamp)

#    for timestamp in allTimestamps:
#        print(timestamp)
    return allTimestamps

def getLastTimestamp(allTimestamps):
    lastTimestamp = allTimestamps[len(allTimestamps) - 1]
    print('lastTimestamp ' + lastTimestamp)
    return lastTimestamp

def print404(body):
    text = body.splitlines()
    global globalBody
    globalBody = ''
    for line in text:
        if ' 404 ' in line:
            globalBody += line + '\n'


def getRequest(urlPartial):
    try:
        response = requests.get(url = "https://www.edmunds.com" + str(urlPartial))
        global globalResponseCode
        global globalBody
        global globalResponseNormal

        globalResponseCode = "response is " + str(response.status_code) + " history " + str(response.history)
        if (response.status_code == 200):
            if (response.history):
                globalResponseNormal = False
            else:
                globalResponseNormal = True
        else:
            globalResponseNormal = False
        printFileResponse()

        try:
            body = response.json()
#            print body
            globalBody = "json not being printed"
        #        print404(body)
            return
        except ValueError:
#            print "Not json"
            pass

        try:
            body = response.text
        #        print body
            print404(body)
            return
        except Exception as wtf:
            print "response.text failed "
            print wtf
    except TypeError as typeError:
        globalBody = "Problem with urlPartial " + str(typeError)
    except requests.exceptions.ConnectionError as connectionError:
        globalBody = "Problem with urlPartial connection " + str(connectionError)
    except requests.exceptions.InvalidURL as invalidURL:
        globalBody = "Problem with urlPartial " + str(invalidURL)

    printFileResponse()

clientS3 = boto3.client('s3')
resourceS3 = boto3.resource('s3')

#for bucket in resourceS3.buckets.all():
#    print(bucket.name)

myBucket = 'edmunds-perftesting-testplans'

bucket = resourceS3.Bucket(myBucket)
environment = 'QA-11'
allObjects = listAllObjects(bucket, environment)
allTimestamps = getAllTimestamps(allObjects)
lastTimestamp = getLastTimestamp(allTimestamps)

files = getFiles(bucket, environment, lastTimestamp)
for file in files:
    filename = file.key
    if filename.endswith('csv'):
        body = readFile(bucket, environment, lastTimestamp, filename)
        if (body is None):
            printFileResponse()
            continue
        firstLine = setFirstLine(filename, body)
        if (firstLine is None):
            printFileResponse()
            continue
        getRequest(firstLine)
        printFileResponse()
