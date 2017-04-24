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
    return

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


def getEmptyContainers(ec2ContainersSorted):
    empty = []
    for container in ec2ContainersSorted:
        if (container.runningTasksCount > 0):
            return empty
        empty.append(container)
    return empty


def addSome(numberToAdd):
    print "write some code to add", numberToAdd
    return

def deleteSome(containersToDelete):
    print "write some code to delete", str(len(containersToDelete))
    for container in containersToDelete:
        container.printMe()
    return


def doit(clusterName):
    # Describe all instances in the ECS cluster
    containerInstancesArns = getInstanceArns(clusterName)
    containerInstances = getContainerInstances(clusterName, containerInstancesArns)
    print "number containerInstances  " + str(len(containerInstances))

    # Sort instances by number of running tasks
    sortedContainerInstances = sorted(
        containerInstances, 
        key=lambda containerInstances: containerInstances['runningTasksCount']
    )

#    if (verbose):
#        for instance in sortedContainerInstances:
#            printInstance(instance)

    ec2Containers = []
    for instance in sortedContainerInstances:
        ec2Containers.append(createEC2Container(instance, clusterName))

    emptyContainers = getEmptyContainers(ec2Containers)
    numberEmpty = len(emptyContainers)
    print "Number of EC2 Containers", str(len(ec2Containers))
    print "Number of empty EC2 Containers", str(numberEmpty)
    if (numberEmpty < minContainersWanted):
        addSome(minContainersWanted - numberEmpty)
    elif (numberEmpty > maxContainersWanted):
        numberToDelete = numberEmpty - maxContainersWanted
        deleteMe = []
        for i in range(0, numberToDelete):
            deleteMe.append(emptyContainers[i])
        print "will delete", str(len(deleteMe))
        deleteSome(deleteMe)
    else:
        print "We are currently good, we have", numberEmpty, "and min is", minContainersWanted, "max", maxContainersWanted
    return


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
    parser.add_argument('-m', '--min', 
        required=True,
        help='The minimum number of containers wanted'
    )
    parser.add_argument('-M', '--max', 
        required=True,
        help='The maximum number of containers wanted'
    )
    parser.add_argument('-v', '--verbose',
        nargs='?',
        required=False,
        default='False',
        help='Guess'
    )
    args = parser.parse_args()  
    regionName = args.region

    global ecs
    ecs = boto3.client('ecs', region_name=regionName)
    global verbose
    verbose = (args.verbose == 'True')
    global taskDescriptions
    taskDescriptions = None
    global minContainersWanted
    minContainersWanted = int(args.min)
    global maxContainersWanted
    maxContainersWanted = int(args.max)


    logging.info('Autoscaling cluster %s on region %s...', args.cluster, regionName)
    doit(args.cluster)
