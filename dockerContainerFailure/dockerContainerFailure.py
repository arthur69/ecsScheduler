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


def unableHandler(event, context):
    global verbose
    verbose = False
    global service
    service = "all"
    global liveEnvironment
    liveEnvironment = getLiveEnvironment()

    clusters = createClusters()
    for cluster in clusters:
        print "doing cluster", cluster.toString()

        global ecs
        ecs = boto3.client('ecs', region_name=cluster.region)
        doit(cluster.name, cluster.region)
    return


def createClusters():
    clusters = []
    clusters.append(clusterDefinition("qa-11", "us-east-1"))
    clusters.append(clusterDefinition("prod-11", "us-east-1"))
    clusters.append(clusterDefinition("qa-21", "us-west-2"))
    clusters.append(clusterDefinition("prod-21", "us-west-2"))
    return clusters


def outputToWavefrontService(cluster, region, serviceName, count):
    createAndSendMetric(cluster, region, "." + serviceName, count)
    return


def outputToWavefrontNoService(cluster, region, count):
    createAndSendMetric(cluster, region, "", count)
    return


def createAndSendMetric(cluster, region, serviceNameIfExists, count):
    #com.edmunds.ops.aws.ecs.{REGION}.{CLUSTER_NAME}.{SERVICE_NAME}.tasks.error.{ERROR-KEY}.count
    metric = "com.edmunds.ops.aws.ecs." + region + "." + cluster + serviceNameIfExists + ".tasks.error.unabletoplace.count"
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


def getServices(clusterName):
#    print "getServices"
#response = client.list_services(
#    cluster='string',
#    nextToken='string',
#    maxResults=123
#)
    services = []
    response = ecs.list_services(cluster=clusterName)
#    print "Got one cluster " + clusterName
#    print response
    services.extend(response['serviceArns'])

    # If there are more services, keep retrieving them
    while response.get('nextToken', None) is not None:
        response = ecs.list_services(
            cluster=clusterName,
            nextToken=response['nextToken']
        )
#        print "Getting more cluster " + clusterName
#        print response
        services.extend(response['serviceArns'])

    if (verbose):
        print "number services read " + str(len(services))

    return services


def unableEvents(events, serviceName):
    if (verbose):
        print "events"

    printServiceName = True
    numberFound = 0
    for event in events:
        message = event['message']
        if ("unable" not in message):
            continue

        createdAt = str(event['createdAt'])
        if ((monthDayToday in createdAt) or (monthDayYesterday in createdAt)):
            numberFound += 1
            if (printServiceName):
                print "serviceName", serviceName
                printServiceName = False

            print "\tmessage", message
            print "\tcreatedAt", createdAt, "\n"
    return numberFound


def unableServices(services):
    if (verbose):
        print "services"

    numberFound = 0
    for service in services:
        serviceName = service['serviceName']
        if (verbose):
            print service
            print "serviceName", serviceName

        numberFound += unableEvents(service['events'], serviceName)
    return numberFound


def findUnable(clusterName, service):
    if (verbose):
        print "findUnable", service

    services = []
    services.append(service)
    response = ecs.describe_services(cluster=clusterName, services=services)
    return unableServices(response['services'])


def printEvents(events):
    print "events"
    for event in events:
        print "\tmessage", event['message']
#        print "\tid", event['id']
        print "\tcreatedAt", event['createdAt'], "\n"
    return


def printServices(services):
    for service in services:
#        print service
        print "serviceName", service['serviceName']
        print "runningCount", service['runningCount'], "desiredCount", service['desiredCount'], "pendingCount", service['pendingCount']
        print "placementConstraints", service['placementConstraints'], "placementStrategy", service['placementStrategy']
        print "status", service['status']
        print "taskDefinition", service['taskDefinition']
        print "loadBalancers", service['loadBalancers']
#        print "roleArn", service['roleArn']
        print "createdAt", service['createdAt']
#        print "clusterArn", service['clusterArn']
#        print "serviceArn", service['serviceArn']
        print "deploymentConfiguration", service['deploymentConfiguration']
#        print "deployments", service['deployments']
        printEvents(service['events'])
    return


def printService(clusterName, service):
    print "printService", service
    services = []
    services.append(service)
    response = ecs.describe_services(cluster=clusterName, services=services)
    #    for k, v in response.items():
    #        print "key " + k
    #        print(k, v)
    printServices(response['services'])
    return


def setDates():
    today = datetime.now()
#    print "Today " + today.strftime('%Y-%m-%d %H:%M:%S')
    global monthDayToday
    monthDayToday = today.strftime('%Y-%m-%d')
#    print "monthDay", monthDayToday
    yesterday = today - timedelta(days=1)
#    print "Yesterday " + yesterday.strftime('%Y-%m-%d %H:%M:%S')
    global monthDayYesterday
    monthDayYesterday = yesterday.strftime('%Y-%m-%d')
#    print "monthDayYesterday", monthDayYesterday
    return


def doService(clusterName, region):
    if (verbose):
        print "doService", service

    setDates()

    if ((service != "ALL") and (service != "all")):
        if (verbose):
            printService(clusterName, service)
        numberFound = findUnable(clusterName, service)
        if (numberFound > 0):
            print "For service " + service + " we found " + str(numberFound) + " failed tasks"
        return numberFound

    if (verbose):
        print "doAll"

    numberFound = 0
    services = getServices(clusterName)
    for serviceLocal in services:
        if (verbose):
            printService(clusterName, serviceLocal)
        numberFoundService = findUnable(clusterName, serviceLocal)
        numberFound += numberFoundService
        serviceName = serviceLocal.split("/")[1]
        if (numberFoundService > 0):
            print "For service " + serviceName + " we found " + str(numberFoundService) + " failed tasks"
            outputToWavefrontService(clusterName, region, serviceName, numberFoundService)
#    services.append(service)
#    response = ecs.describe_services(cluster=clusterName, services=services)
#    for k, v in response.items():
#        print "key " + k
#        print(k, v)
#    printServices(response['services'])
    if (numberFound > 0):
        print "Total found " + str(numberFound) + " failed tasks"
        outputToWavefrontNoService(clusterName, region, numberFound)
    return numberFound


def doit(clusterName, region):
    numberFound = doService(clusterName, region)
    if (numberFound > 0):
        print "Overall we found " + str(numberFound) + " failed tasks"
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
    parser.add_argument('-s', '--service',
                        nargs='?',
                        required=False,
                        default=None,
                        help='Will process the requested service, all is allowed'
    )
    parser.add_argument('-l', '--lambdaAws',
                        nargs='?',
                        required=False,
                        default=None,
                        help='Whether to run Lambda'
    )
    args = parser.parse_args()

    if (args.lambdaAws == 'True'):
        event = ['dummy event']
        context = None
        unableHandler(event, context)
    else:
        regionName = args.region
        global ecs
        ecs = boto3.client('ecs', region_name=regionName)
        global verbose
        verbose = (args.verbose == 'True')
        global service
        service = args.service
        logging.info('Monitoring cluster %s on region %s...', args.cluster, regionName)
        doit(args.cluster, regionName)

