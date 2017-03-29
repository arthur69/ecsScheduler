#!/usr/bin/env python
import boto3
import argparse
import logging


class EC2Container(object):
    def __init__(self, name):
        self.name = name

    def printMe(self):
        print "EC2Container name " + self.name + " status " + self.status
        print "\tpending " + str(self.pendingTasksCount) + " running " + str(self.runningTasksCount)
        print "\tagentConnected " + str(self.agentConnected) + " instanceArn " + self.instanceArn
        print "\tRegistered cpu " + str(self.registeredCpu) + " memory " + str(self.registeredMemory)
        print "\tRemaining cpu " + str(self.remainingCpu) + " memory " + str(self.remainingMemory)

    def setRegisteredResources(self, registeredResources):
        for resource in registeredResources:
            if (resource['name'] == 'CPU'):
                self.registeredCpu = resource['integerValue']
            elif (resource['name'] == 'MEMORY'):
                self.registeredMemory = resource['integerValue']
            elif (resource['name'] == 'PORTS'):
                pass
            elif (resource['name'] == 'PORTS_UDP'):
                pass
            else:
                print "Error in EC2Container.setRegisteredResources"
                print resource
                for k, v in resource.items():
                    print "resourceKey " + k
                    print(k, v)

    def setRemainingResources(self, remainingResources):
        for resource in remainingResources:
            if (resource['name'] == 'CPU'):
                self.remainingCpu = resource['integerValue']
            elif (resource['name'] == 'MEMORY'):
                self.remainingMemory = resource['integerValue']
            elif (resource['name'] == 'PORTS'):
                pass
            elif (resource['name'] == 'PORTS_UDP'):
                pass
            else:
                print "Error in EC2Container.setRemainingResources"
                print resource
                for k, v in resource.items():
                    print "resourceKey " + k
                    print(k, v)

class Statistics(object):
    def __init__(self, name):
        self.name = name
        self.count = 0

    def printMe(self):
        print "Statistics", self.name, "Count", self.count, "cpuLeft", self.cpuLeft, "memoryLeft", self.memoryLeft, "tasksRunning", self.tasksRunning

    def populate(self, ec2Container):
        if (self.count > 0):
            self.count += 1
            return
        self.count = 1
        self.cpuLeft = ec2Container.remainingCpu
        self.memoryLeft = ec2Container.remainingMemory
        self.tasksRunning = ec2Container.runningTasksCount

def createStatisticsName(ec2Container):
    return "Stat" + str(ec2Container.runningTasksCount) + "|" + str(ec2Container.remainingCpu) + "|" + str(ec2Container.remainingMemory)

# Set up logger
logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

def getInstanceArns(clusterName):
    containerInstancesArns = []
    # Get instances in the cluster
    response = ecs.list_container_instances(cluster=clusterName)
    containerInstancesArns.extend(response['containerInstanceArns'])
#    print "Got one cluster " + clusterName
#    print response

    # If there are more instances, keep retrieving them
    while response.get('nextToken', None) is not None:
        response = ecs.list_container_instances(
            cluster=clusterName,
            nextToken=response['nextToken']
        )
#        print "Getting more cluster " + clusterName
#        print response
        containerInstancesArns.extend(response['containerInstanceArns'])

    print "number containers read " + str(len(containerInstancesArns))
    return containerInstancesArns

def printRegisteredResources(registeredResources):
    print "registeredResources"
    for resource in registeredResources:
        if (resource['name'] == 'CPU'):
            print "CPU " + str(resource['integerValue'])
        elif (resource['name'] == 'MEMORY'):
            print "MEMORY " + str(resource['integerValue'])
        elif (resource['name'] == 'PORTS'):
#            print "PORTS " + str(resource['stringSetValue'])
            pass
        elif (resource['name'] == 'PORTS_UDP'):
#            print "PORTS_UDP " + str(resource['stringSetValue'])
            pass
        else:
            print resource
            for k, v in resource.items():
                print "resourceKey " + k
                print(k, v)

def printRemainingResources(remainingResources):
    print "remainingResources"
    for resource in remainingResources:
        if (resource['name'] == 'CPU'):
            print "CPU " + str(resource['integerValue'])
        elif (resource['name'] == 'MEMORY'):
            print "MEMORY " + str(resource['integerValue'])
        elif (resource['name'] == 'PORTS'):
#            print "PORTS " + str(resource['stringSetValue'])
            pass
        elif (resource['name'] == 'PORTS_UDP'):
#            print "PORTS_UDP " + str(resource['stringSetValue'])
            pass
        else:
            print resource
            for k, v in resource.items():
                print "resourceKey " + k
                print(k, v)

def printAttributes(attributes):
    print "attributes"
    for attribute in attributes:
        print attribute

def printVersionInfo(versionInfo):
    print "versionInfo"
    for info in versionInfo:
        print info + " " + versionInfo[info]

def printInstance(instance):
    print "instance"
#    for k, v in instance.items():
#        print "key " + k
#        print(k, v)

    print "status is " + instance['status']
    printRegisteredResources(instance['registeredResources'])
    print "ec2InstanceId is " + instance['ec2InstanceId']
    print "agentConnected is " + str(instance['agentConnected'])
    print "containerInstanceArn is " + instance['containerInstanceArn']
    print "pendingTasksCount is " + str(instance['pendingTasksCount'])
    printRemainingResources(instance['remainingResources'])
#    print "version is " + str(instance['version'])
#    printAttributes(instance['attributes'])
#    printVersionInfo(instance['versionInfo'])
    print "runningTasksCount is " + str(instance['runningTasksCount'])

def getTaskDefinitionId(taskDescription):
    taskDefinitionLong = taskDescription['taskDefinitionArn']
#    print "taskDefinitionLong", taskDefinitionLong
    tokens = taskDefinitionLong.split("/")
#    print "taskDefinitionShort", tokens[1]
    return tokens[1]


def outputAllInstanceEC2Container(instance, ec2Container, clusterName):
    taskDescriptions = getTaskDescriptionsIfNeeded(clusterName)
    printInstance(instance)
    ec2Container.printMe()
    for taskDescription in taskDescriptions:
        if (taskDescription['containerInstanceArn'] == instance['containerInstanceArn']):
            printTaskDescription(taskDescription)
            taskDefinitionId = getTaskDefinitionId(taskDescription)
            printTaskDefinition(taskDefinitionId)

def createEC2Container(instance, clusterName):
#    print "createEC2Container"
#    if verbose:
#        printInstance(instance)

    ec2Container = EC2Container(instance['ec2InstanceId'])
    ec2Container.status = instance['status']
    ec2Container.setRegisteredResources(instance['registeredResources'])
    ec2Container.agentConnected = instance['agentConnected']
    ec2Container.instanceArn = instance['containerInstanceArn']
    ec2Container.pendingTasksCount = instance['pendingTasksCount']
    ec2Container.setRemainingResources(instance['remainingResources'])
    ec2Container.runningTasksCount = instance['runningTasksCount']
    if verbose:
        ec2Container.printMe()

    if ((ec2Container.remainingCpu > 4000) and (ec2Container.remainingCpu < 4096)):
        print "WARNING This is an odd instance and container"
        outputAllInstanceEC2Container(instance, ec2Container, clusterName)

    return ec2Container

def getContainerInstances(clusterName, containerInstancesArns):
    containerInstances = []
    start = 0
    while (start < len(containerInstancesArns)):
        end = min(start + 100, len(containerInstancesArns))
        smallContainerArns = containerInstancesArns[start:end]
        response = ecs.describe_container_instances(
            cluster=clusterName,
            containerInstances=smallContainerArns
            )
        containerInstances.extend(response['containerInstances'])
        start += 100

    return containerInstances

def createStatistics(ec2Containers):
    statistics = []
    for ec2Container in ec2Containers:
        name = createStatisticsName(ec2Container)
        foundIt = False
        for statistic in statistics:
            if (name == statistic.name):
                foundIt = True
                break
        if (foundIt == False):
            statistic = Statistics(name)
            statistics.append(statistic)
        statistic.populate(ec2Container)

    for statistic in statistics:
        statistic.printMe()


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

    print "number services read " + str(len(services))
    return services


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


def getTaskDescriptionsIfNeeded(clusterName):
    global taskDescriptions
    if (taskDescriptions is None):
        tasks = getTasks(clusterName)
        taskDescriptions = getTaskDescriptions(clusterName, tasks)
    return taskDescriptions


def listTasks(clusterName):
#    print "listTasks"
    taskDescriptions = getTaskDescriptionsIfNeeded(clusterName)
    taskDescriptionsSorted = sorted(
        taskDescriptions,
        key=lambda taskDescriptions: taskDescriptions['taskDefinitionArn']
        )

    count = 0
    lastSeen = None
    numberUnique = 0
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

    print "Number tasks " + str(len(taskDescriptions)) + " and unique tasks " + str(numberUnique)
    return


def unableEvents(events):
    print "events"
    for event in events:
        message = event['message']
        if ("unable" not in message):
            continue
        print "\tmessage", message
        #        print "\tid", event['id']
        print "\tcreatedAt", event['createdAt'], "\n"
    return


def unableServices(services):
    #    print "services"
    for service in services:
        #        print service
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
        unableEvents(service['events'])
    return



def findUnable(clusterName, service):
    print "findUnable", service
    services = []
    services.append(service)
    response = ecs.describe_services(cluster=clusterName, services=services)
    #    for k, v in response.items():
    #        print "key " + k
    #        print(k, v)
    unableServices(response['services'])
    return


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


def doService(clusterName):
    print "doService", service
    if ((service != "ALL") and (service != "all")):
#        printService(clusterName, service)
        findUnable(clusterName, service)
        return

    print "doAll"
    services = getServices(clusterName)
    for serviceLocal in services:
#        printService(clusterName, serviceLocal)
        findUnable(clusterName, service)
#    services.append(service)
#    response = ecs.describe_services(cluster=clusterName, services=services)
#    for k, v in response.items():
#        print "key " + k
#        print(k, v)
#    printServices(response['services'])
    return

def doit(clusterName):
    doService(clusterName)
    return

# 
# ECS Scheduler
#
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='ECS Custom Scheduler to start a task on the instance with the least number of running tasks.'
    )
    parser.add_argument('-c', '--cluster', 
        required=True,
        help='The short name or full Amazon Resource Name (ARN) of the cluster that you want to start your task on.'
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
    global taskDescriptions
    taskDescriptions = None
    global service
    service = args.service

    logging.info('Monitoring cluster %s on region %s...', args.cluster, regionName)
    doit(args.cluster)
