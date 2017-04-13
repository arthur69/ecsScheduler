#!/usr/bin/env python
import boto3
import argparse
import logging
from datetime import datetime, timedelta
import json
import time
import requests
import socket


# Set up logger
logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)


class clusterDefinition(object):
    def __init__(self, name, region):
        self.name = name
        self.region = region
        return

    def toString(self):
        return "Cluster name " + self.name + " region " + self.region


def memoryHandler(event, context):
    global verbose
    verbose = False
    global liveEnvironment
    liveEnvironment = getLiveEnvironment()

    clusters = createClusters()
    for cluster in clusters:
        print "doing cluster", cluster.toString()

        global ecs
        ecs = boto3.client('ecs', region_name=cluster.region)
        global taskDescriptions
        taskDescriptions = None
        doit(cluster.name, cluster.region)
    return


def createClusters():
    clusters = []
    clusters.append(clusterDefinition("qa-11", "us-east-1"))
    clusters.append(clusterDefinition("prod-11", "us-east-1"))
    clusters.append(clusterDefinition("qa-21", "us-west-2"))
    clusters.append(clusterDefinition("prod-21", "us-west-2"))
    return clusters


def outputToWavefrontTask(cluster, region, taskName, count):
    createAndSendMetric(cluster, region, "." + taskName, count)
    return

def outputToWavefrontNoTask(cluster, region, count):
    createAndSendMetric(cluster, region, "", count)
    return

def createAndSendMetric(cluster, region, taskNameIfExists, count):
    #com.edmunds.ops.aws.ecs.{REGION}.{CLUSTER_NAME}.{SERVICE_NAME}.tasks.error.{ERROR-KEY}.count
    metric = "com.edmunds.ops.aws.ecs." + region + "." + cluster + taskNameIfExists + ".tasks.error.containerMemoryTooSmall.count"
    if (liveEnvironment == 'prod-21'):
        proxy_url = 'wavefront-proxy.prod-21.vip.aws2'
    else:
        proxy_url = 'wavefront-proxy.prod-11.vip.aws1'

    s = socket.socket()
    s.connect((proxy_url, 2878))
    timestamp = int(time.time())
    message = metric + " %d %d source=%s\n" % (count, timestamp, liveEnvironment)
    print "Sending to Wavefront " + message
    s.send(message)
    s.close()
    return


def getLiveEnvironment():
    request = requests.get("http://emon-api.prod-admin11.vip.aws1/api/environments")

    results = json.loads(request.text)

    for env in results['data']:
        if (env['type'] == "media"):
            if (env['is_live'] == True):
                return env['name']
    return "NothingLive"


def getTaskDefinitionId(taskDescription):
    taskDefinitionLong = taskDescription['taskDefinitionArn']
    #    print "taskDefinitionLong", taskDefinitionLong
    tokens = taskDefinitionLong.split("/")
    #    print "taskDefinitionShort", tokens[1]
    return tokens[1]


def getTasks(clusterName):
    #    print "getTasks"
    #response = client.list_tasks(
    #    cluster='string',
    #    containerInstance='string',
    #    family='string',
    #    nextToken='string',
    #    maxResults=123,
    #    startedBy='string',
    #    serviceName='string',
    #    desiredStatus='RUNNING'|'PENDING'|'STOPPED'
    #)
    tasks = []
    response = ecs.list_tasks(cluster=clusterName)
    #    print "Got one cluster " + clusterName
    #    print response
    tasks.extend(response['taskArns'])

    # If there are more tasks, keep retrieving them
    while response.get('nextToken', None) is not None:
        response = ecs.list_tasks(
            cluster=clusterName,
            nextToken=response['nextToken']
        )
        #        print "Getting more cluster " + clusterName
        #        print response
        tasks.extend(response['taskArns'])

    #    print "number tasks read " + str(len(tasks))
    return tasks


def getTaskDefinition(taskDefinition):
    #    print "getTaskDefinition", taskDefinition
    response = ecs.describe_task_definition(taskDefinition=taskDefinition)
    #    print response
    return response['taskDefinition']


def printContainerDefinitionEnvironment(environment):
    print "\t\tcontainerDefinitionEnvironment"
    for keyValue in environment:
        print "\t\t\tname",keyValue['name'],"value",keyValue['value']
    return


def getMaxMemoryJava(environment):
    for keyValue in environment:
        if (keyValue['name'] == "JAVAMAXMEM"):
            value =keyValue['value']
            if ('m' in value):
                stripMeg = value.strip("m")
            elif ('M' in value):
                stripMeg = value.strip("M")
            else:
                print "Problem finding multiplier in JAVAMAXMEM " + value
                return 0
            try:
                return int(stripMeg)
            except:
                return 0

    return 0


def printInterestingContainerDefinition(containerDefinition):
    if (containerDefinition['cpu'] < 512):
        print "Small cpu"
    memoryContainer = containerDefinition['memory']
    memoryJava = getMaxMemoryJava(containerDefinition['environment'])
    memoryLeft = memoryContainer - memoryJava
    memory80 = memoryContainer * 0.2
    if (memoryLeft < memory80):
        print "Memory may be too constrained container", memoryContainer, "java", memoryJava, "diff", memoryLeft, "would like", memory80


def printContainerDefinitions(containerDefinitions):
    #    print "containerDefinitions", containerDefinitions
    for containerDefinition in containerDefinitions:
        print "\tcontainerDefinition"
        #        print containerDefinition
        print "\t\tname",containerDefinition['name']
        print "\t\tmountPoints",containerDefinition['mountPoints']
        print "\t\timage",containerDefinition['image']
        print "\t\tcpu",containerDefinition['cpu']
        print "\t\tportMappings",containerDefinition['portMappings']
        try:
            print "\t\tulimits",containerDefinition['ulimits']
        except KeyError:
            print "Does not exist"
        print "\t\tmemory",containerDefinition['memory']
        print "\t\tessential",containerDefinition['essential']
        print "\t\tvolumesFrom",containerDefinition['volumesFrom']
        printContainerDefinitionEnvironment(containerDefinition['environment'])
        printInterestingContainerDefinition(containerDefinition)
    return


def printTaskDefinition(taskDefinitionId):
    print "\ttaskDefinition", taskDefinitionId
    taskDefinition = getTaskDefinition(taskDefinitionId)
    #    print taskDefinition
    print "\tstatus",taskDefinition['status']
    print "\tfamily",taskDefinition['family']
    print "\tplacementConstraints",taskDefinition['placementConstraints']
    try:
        print "\trequiresAtrributes",taskDefinition['requiresAttributes']
    except KeyError:
        print "Nothing required"
    print "\tvolumes",taskDefinition['volumes']
    print "\ttaskDefinitionArn",taskDefinition['taskDefinitionArn']
    printContainerDefinitions(taskDefinition['containerDefinitions'])
    print "\trevision",taskDefinition['revision']
    return

def getTaskDescriptions(clusterName, tasks):
    #    print "getTaskDescriptions"
    taskDescriptions = []
    start = 0
    while (start < len(tasks)):
        end = min(start + 100, len(tasks))
        smallTasks = tasks[start:end]
        response = ecs.describe_tasks(cluster=clusterName, tasks=smallTasks)
        #    print "Got one cluster " + clusterName
        #    print response
        taskDescriptions.extend(response['tasks'])
        start += 100

    #    print "number taskDescriptions read " + str(len(taskDescriptions))
    return taskDescriptions


def printTaskDescription(taskDescription):
    print "\ntaskDescription", taskDescription['taskDefinitionArn']
    #    print taskDescription
    #    for k, v in taskDescription.items():
    #        print "key " + k
    #        print(k, v)
    #    print "taskArn", taskDescription['taskArn']
    print "\tlastStatus", taskDescription['lastStatus'], "desiredStatus", taskDescription['desiredStatus']
    print "\tgroup", taskDescription['group']
    #    print "\toverrides", taskDescription['overrides']
    #    print "\tcontainerInstanceArn", taskDescription['containerInstanceArn']
    if (taskDescription['lastStatus'] != "PENDING"):
        print "\tcreatedAt", taskDescription['createdAt'], "startedAt", taskDescription['startedAt']
    else:
        print "\tcreatedAt", taskDescription['createdAt'], "startedAt", "NOT started yet"

    #    print "\tversion", taskDescription['version']
    #    print "\tclusterArn", taskDescription['clusterArn']
    #    print "\tstartedBy", taskDescription['startedBy']
    #    print "\tcontainers", taskDescription['containers']
    taskDefinitionId = getTaskDefinitionId(taskDescription)
    printTaskDefinition(taskDefinitionId)
    #    raise SystemExit
    return


def isUndersizedInterestingContainerDefinition(containerDefinition):
#    if (containerDefinition['cpu'] < 512):
#        print "Small cpu"
    memoryContainer = containerDefinition['memory']
    memoryJava = getMaxMemoryJava(containerDefinition['environment'])
    memoryLeft = memoryContainer - memoryJava
    memory70 = memoryContainer * 0.3
    if (memoryLeft < memory70):
        print "Memory may be too constrained container", memoryContainer, "java", memoryJava, "diff", memoryLeft, "would like", memory70
        return True

    return False


def isUndersizedContainerDefinitions(containerDefinitions):
    isUndersizedDefinition = False
    for containerDefinition in containerDefinitions:
#        print "\tcontainerDefinition"
#        print "\t\tname",containerDefinition['name']
#        print "\t\tmountPoints",containerDefinition['mountPoints']
#        print "\t\timage",containerDefinition['image']
#        print "\t\tcpu",containerDefinition['cpu']
#        print "\t\tportMappings",containerDefinition['portMappings']
#        try:
#            print "\t\tulimits",containerDefinition['ulimits']
#        except KeyError:
#            print "Does not exist"
#        print "\t\tmemory",containerDefinition['memory']
#        print "\t\tessential",containerDefinition['essential']
#        print "\t\tvolumesFrom",containerDefinition['volumesFrom']
#        printContainerDefinitionEnvironment(containerDefinition['environment'])
        isUndersized = isUndersizedInterestingContainerDefinition(containerDefinition)
        if (isUndersized):
            isUndersizedDefinition = True
    return isUndersizedDefinition


def isUndersizedTaskDefinition(taskDefinitionId):
#    print "\tisUndersizedtaskDefinition", taskDefinitionId
    taskDefinition = getTaskDefinition(taskDefinitionId)
#    print "\tstatus",taskDefinition['status']
#    print "\tfamily",taskDefinition['family']
#    print "\tplacementConstraints",taskDefinition['placementConstraints']
#    try:
#        print "\trequiresAtrributes",taskDefinition['requiresAttributes']
#    except KeyError:
#        print "Nothing required"
#    print "\tvolumes",taskDefinition['volumes']
#    print "\ttaskDefinitionArn",taskDefinition['taskDefinitionArn']
    isUndersized = isUndersizedContainerDefinitions(taskDefinition['containerDefinitions'])
#    print "\trevision",taskDefinition['revision']
    return isUndersized


def isUndersizedTaskDescription(taskDescription):
#    print "\nisUndersizedTaskDescription", taskDescription['taskDefinitionArn']
#    print "\tlastStatus", taskDescription['lastStatus'], "desiredStatus", taskDescription['desiredStatus']
#    print "\tgroup", taskDescription['group']
#    if (taskDescription['lastStatus'] != "PENDING"):
#        print "\tcreatedAt", taskDescription['createdAt'], "startedAt", taskDescription['startedAt']
#    else:
#        print "\tcreatedAt", taskDescription['createdAt'], "startedAt", "NOT started yet"

    taskDefinitionId = getTaskDefinitionId(taskDescription)
    isUndersized = isUndersizedTaskDefinition(taskDefinitionId)
    #    raise SystemExit
    return isUndersized


def getTaskDescriptionsIfNeeded(clusterName):
    global taskDescriptions
    if (taskDescriptions is None):
        tasks = getTasks(clusterName)
        taskDescriptions = getTaskDescriptions(clusterName, tasks)
    return taskDescriptions


def getTaskName(taskDescription):
    taskNamePreColon = taskDescription['taskDefinitionArn'].split("/")[1]
    taskName = taskNamePreColon.split(":")[0]
    return taskName


def doTasks(clusterName, region):
    #    print "doTasks"
    taskDescriptions = getTaskDescriptionsIfNeeded(clusterName)
    taskDescriptionsSorted = sorted(
        taskDescriptions,
        key=lambda taskDescriptions: taskDescriptions['taskDefinitionArn']
    )

    count = 0
    lastSeen = None
    numberUnique = 0
    numberTooSmall = 0
    for taskDescription in taskDescriptionsSorted:
        if (taskDescription['taskDefinitionArn'] == lastSeen):
            count += 1
        else:
            if (count > 1):
                if (verbose):
                    print "\tTimes " + str(count)
            count = 1
            numberUnique += 1
            lastSeen = taskDescription['taskDefinitionArn']
            if (verbose):
                printTaskDescription(taskDescription)
            if (isUndersizedTaskDescription(taskDescription)):
                numberTooSmall += 1
                taskName = getTaskName(taskDescription)
                print "For task " + taskName + " we found too small"
                outputToWavefrontTask(clusterName, region, taskName, count)

    outputToWavefrontNoTask(clusterName, region, numberTooSmall)
    print "Number tasks " + str(len(taskDescriptions)) + " and unique tasks " + str(numberUnique) + " and too small " + str(numberTooSmall)
    return


def doit(clusterName, regionName):
    doTasks(clusterName, regionName)
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Monitor looking for unable to place tasks.'
    )
    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='The short name or full Amazon Resource Name (ARN) of the cluster.'
    )
    parser.add_argument('-r', '--region',
                        required=True,
                        help='The region that we are talking about'
    )
    parser.add_argument('-v', '--verbose',
                        nargs='?',
                        required=False,
                        default='False',
                        help='Guess'
    )
    parser.add_argument('-t', '--tasks',
                        nargs='?',
                        required=False,
                        default='False',
                        help='Will list the current running and pending tasks'
    )
    parser.add_argument('-l', '--lambdaAws',
                        nargs='?',
                        required=False,
                        default=None,
                        help='Whether to run Lambda'
    )
    args = parser.parse_args()
    regionName = args.region

    global ecs
    ecs = boto3.client('ecs', region_name=regionName)
    global verbose
    verbose = (args.verbose == 'True')
    global taskDescriptions
    taskDescriptions = None

    if (args.lambdaAws == 'True'):
        event = ['dummy event']
        context = None
        memoryHandler(event, context)
    else:
        logging.info('Monitoring cluster %s on region %s...', args.cluster, regionName)
        doit(args.cluster, regionName)
