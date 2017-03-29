#!/usr/bin/env python
import boto3
import argparse
import logging
from datetime import datetime, timedelta

# Set up logger
logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)


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


def unableEvents(events):
    if (verbose):
        print "events"

    numberFound = 0
    for event in events:
        message = event['message']
        if ("unable" not in message):
            continue

        createdAt = str(event['createdAt'])
        if ((monthDayToday in createdAt) or (monthDayYesterday in createdAt)):
            numberFound += 1
            if (verbose):
                print "\tmessage", message
                print "\tcreatedAt", createdAt, "\n"
    return numberFound


def unableServices(services):
    if (verbose):
        print "services"

    numberFound = 0
    for service in services:
        if (verbose):
            print service
            print "serviceName", service['serviceName']
#        print "runningCount", service['runningCount'], "desiredCount", service['desiredCount'], "pendingCount", service['pendingCount']
#        print "placementConstraints", service['placementConstraints'], "placementStrategy", service['placementStrategy']
#        print "status", service['status']
#        print "taskDefinition", service['taskDefinition']
#        print "loadBalancers", service['loadBalancers']
#        #        print "roleArn", service['roleArn']
#        print "createdAt", service['createdAt']
#        #        print "clusterArn", service['clusterArn']
#        #        print "serviceArn", service['serviceArn']
#        print "deploymentConfiguration", service['deploymentConfiguration']
#        #        print "deployments", service['deployments']
        numberFound += unableEvents(service['events'])
    return numberFound


def findUnable(clusterName, service):
    if (verbose):
        print "findUnable", service

    services = []
    services.append(service)
    response = ecs.describe_services(cluster=clusterName, services=services)
    #    for k, v in response.items():
    #        print "key " + k
    #        print(k, v)
    return unableServices(response['services'])


def printEvents(events):
    print "events"
    for event in events:
        print "\tmessage", event['message']
#        print "\tid", event['id']
        print "\tcreatedAt", event['createdAt'], "\n"
    return


def printServices(services):
#    print "services"
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


def doService(clusterName):
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
        if (numberFoundService > 0):
            print "For service " + serviceLocal.split("/")[1] + " we found " + str(numberFoundService) + " failed tasks"
#    services.append(service)
#    response = ecs.describe_services(cluster=clusterName, services=services)
#    for k, v in response.items():
#        print "key " + k
#        print(k, v)
#    printServices(response['services'])
    return numberFound

def doit(clusterName):
    numberFound = doService(clusterName)
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
        help='Will list the requested service'
    )
    args = parser.parse_args()  
    regionName = args.region

    global ecs
    ecs = boto3.client('ecs', region_name=regionName)
    global verbose
    verbose = (args.verbose == 'True')
    global service
    service = args.service

    logging.info('Monitoring cluster %s on region %s...', args.cluster, regionName)
    doit(args.cluster)
