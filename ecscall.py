"""
Simple wrapper for spreading a series of function calls across an AWS ECS cluster

"""
import argparse
from concurrent import futures
from multiprocessing.managers import BaseManager
import queue
import threading

import boto3
import cloudpickle


def callFunc(userFunc, argTupleList, numWorkers, ecsClusterParams, callCfg=None):
    """
    Call the given user function repeatedly, with arguments
    from argTupleList. Workers are running calls concurrently
    on an ECS cluster.

    Parameters
    ----------
      userFunc : callable
        The Python function to call
      argTupleList : list of tuples
        List of argument tuples. Each list element is a tuple
        of arguments for userFunc, i.e. userFunc is called
        as userFunc(*argTuple).
      numWorkers : int
        Number of concurrent workers to run
      ecsClusterParams : dict
        Dictionary of parameters to various boto3 calls to manage
        the ECS cluster.
      callCfg : _EcsCallCfg or None
        If given, this allows over-ride of default behaviour of ecscall.
        Mostly timeouts??

    Returns
    -------
      returnValDict : dict
        Dictionary of userFunc return values. Key is the index number
        corresponding to the order in argTupleList. If the call for
        a particular arg tuple did not return (i.e. had an error),
        that index value will be missing from the dictionary.

    """
    if callCfg is None:
        callCfg = _EcsCallCfg()

    clusterMgr = _EcsClusterMgr(userFunc, argTupleList, numWorkers, ecsClusterParams, callCfg)


def makeEcsClusterParams_Fargate():
    ''

def makeEcsClusterParams_PrivateCluster():
    ''

def worker():
    """
    The function which is called by the worker command line entry point
    """


class _EcsCallCfg:
    """
    Control the behaviour of callFunc
    """
    def __init__(self, barrierTimeout=600, waitClusterInstanceCountTimeout=300):
        self.barrierTimeout = barrierTimeout
        self.waitClusterInstanceCountTimeout = waitClusterInstanceCountTimeout


class _EcsClusterMgr:
    """
    Manage the ECS cluster running workers for callFunc
    """
    def __init__(self, userFunc, argTupleList, numWorkers, ecsClusterParams, callCfg):
        self.userFunc = userFunc
        self.argTupleList = argTupleList
        self.numWorkers = numWorkers
        self.ecsClusterParams = ecsClusterParams
        self.callCfg = callCfg

        self.argsQue = queue.Queue()
        self.forceExit = threading.Event()
        self.exceptionQue = queue.Queue()
        self.workerBarrier = threading.Barrier(numWorkers + 1)
        self.returnValQue = queue.Queue()

        # Put all arg tuples into the argsQue (with their index number)
        for i in range(len(argTupleList)):
            argsQue.put((i, argTupleList[i]))

        # Set up the network communication with workers
        self.dataChan = _NetworkDataChannel(userFunc, self.argsQue,
            self.returnValQue, self.forceExit, self.exceptionQue,
            self.workerBarrier)

        self.createdTaskDef = False
        self.createdCluster = False
        self.createdInstances = False
        self.instanceList = None
        self.taskArnList = None

        ecsClient = boto3.client("ecs")
        self.ecsClient = ecsClient

        # Create ECS cluster (if requested)
        try:
            self.createCluster()
            self.runInstances(numWorkers)
        except Exception as e:
            self.shutdownCluster()
            raise e

        # Create the ECS task definition (if requested)
        self.createTaskDef()

        # Now create a task for each compute worker
        runTask_kwArgs = extraParams['run_task']
        runTask_kwArgs['taskDefinition'] = self.taskDefArn
        containerOverrides = runTask_kwArgs['overrides']['containerOverrides'][0]
        if self.createdCluster:
            runTask_kwArgs['cluster'] = self.clusterName

        self.taskArnList = []
        for workerID in range(numWorkers):
            # Construct the command args entry with the current workerID
            workerCmdArgs = ['-i', str(workerID), '--channaddr', channAddr]
            containerOverrides['command'] = workerCmdArgs

            runTaskResponse = ecsClient.run_task(**runTask_kwArgs)
            if len(runTaskResponse['tasks']) > 0:
                taskResp = runTaskResponse['tasks'][0]
                self.taskArnList.append(taskResp['taskArn'])

            failuresList = runTaskResponse['failures']
            if len(failuresList) > 0:
                self.dataChan.shutdown()
                msgList = []
                for failure in failuresList:
                    reason = failure.get('reason', 'UnknownReason')
                    detail = failure.get('detail')
                    msg = "Worker {}: Reason: {}".format(workerID, reason)
                    if detail is not None:
                        msg += "\nDetail: {}".format(detail)
                    msgList.append(msg)
                fullMsg = '\n'.join(msgList)
                raise EcsCallError(fullMsg)

        # Do not proceed until all workers have started
        self.workerBarrier.wait(timeout=callCfg.barrierTimeout)

        # Loop over return values, from returnValQue. Place them in returnValDict and increment
        # the count. We wait on the queue, with a timeout. If timeout is triggered, then 
        # workers have failed, check exception que. 
        

    def shutdown(self):
        """
        Shut down the workers
        """
        # The order in which the various parts are shut down is critical. Please
        # do not change this unless you are really sure.
        # It is also important that all of it happen, so please avoid having
        # any exceptions raised from within this routine.
        self.forceExit.set()
        self.makeOutObjList()
        self.waitClusterTasksFinished()
        self.checkTaskErrors()
        if hasattr(self, 'dataChan'):
            self.dataChan.shutdown()

        if self.createdTaskDef:
            self.ecsClient.deregister_task_definition(taskDefinition=self.taskDefArn)
        # Shut down the ECS cluster, if one was created.
        self.shutdownCluster()

    def shutdownCluster(self):
        """
        Shut down the ECS cluster, if one has been created
        """
        if self.createdInstances and self.instanceList is not None:
            instIdList = [inst['InstanceId'] for inst in self.instanceList]
            self.ec2client.terminate_instances(InstanceIds=instIdList)
            self.waitClusterInstanceCount(self.clusterName, 0)
        if self.createdCluster:
            self.ecsClient.delete_cluster(cluster=self.clusterName)

    def createCluster(self):
        """
        If requested to do so, create an ECS cluster to run on.
        """
        createCluster_kwArgs = self.extraParams.get('create_cluster')
        if createCluster_kwArgs is not None:
            self.clusterName = createCluster_kwArgs.get('clusterName')
            self.ecsClient.create_cluster(**createCluster_kwArgs)
            self.createdCluster = True

    def runInstances(self, numWorkers):
        """
        If requested to do so, run the instances required to populate
        the cluster
        """
        runInstances_kwArgs = self.extraParams.get('run_instances')
        if runInstances_kwArgs is not None:
            self.ec2client = boto3.client('ec2')

            response = self.ec2client.run_instances(**runInstances_kwArgs)
            self.instanceList = response['Instances']
            numInstances = len(self.instanceList)
            self.waitClusterInstanceCount(self.clusterName, numInstances)
            self.createdInstances = True

    def getClusterInstanceCount(self, clusterName):
        """
        Query the given cluster, and return the number of instances it has. If the
        cluster does not exist, return None.
        """
        count = None
        response = self.ecsClient.describe_clusters(clusters=[clusterName])
        if 'clusters' in response:
            for descr in response['clusters']:
                if descr['clusterName'] == clusterName:
                    count = descr['registeredContainerInstancesCount']
        return count

    def getClusterTaskCount(self):
        """
        Query the cluster, and return the number of tasks it has.
        This is the total of running and pending tasks.
        If the cluster does not exist, return None.
        """
        count = None
        clusterName = self.clusterName
        response = self.ecsClient.describe_clusters(clusters=[clusterName])
        if 'clusters' in response:
            for descr in response['clusters']:
                if descr['clusterName'] == clusterName:
                    count = (descr['runningTasksCount'] +
                             descr['pendingTasksCount'])
        return count

    def waitClusterInstanceCount(self, clusterName, endInstanceCount):
        """
        Poll the given cluster until the instanceCount is equal to the
        given endInstanceCount
        """
        instanceCount = self.getClusterInstanceCount(clusterName)
        startTime = time.time()
        timeout = self.callCfg.waitClusterInstanceCountTimeout
        timeExceeded = False

        while ((instanceCount != endInstanceCount) and (not timeExceeded)):
            time.sleep(5)
            instanceCount = self.getClusterInstanceCount(clusterName)
            timeExceeded = (time.time() > (startTime + timeout))

    def waitClusterTasksFinished(self):
        """
        Poll the given cluster until the number of tasks reaches zero
        """
        taskCount = self.getClusterTaskCount()
        startTime = time.time()
        timeout = 600
        timeExceeded = False
        while ((taskCount > 0) and (not timeExceeded)):
            time.sleep(5)
            taskCount = self.getClusterTaskCount()
            timeExceeded = (time.time() > (startTime + timeout))

        # If timeExceeded, then we are somehow in shutdown even though
        # some tasks are still running. In this case, we still want to
        # shut down, so kill off any remaining tasks, so we can still
        # delete the cluster
        if timeExceeded:
            for taskArn in self.taskArnList:
                # We are trying to avoid any exceptions raised from within
                # shutdown, so trap all of them.
                try:
                    self.ecsClient.stop_task(cluster=self.clusterName,
                        task=taskArn, reason="Stopped by shutdown")
                except Exception as e:
                    # I am unsure if I should just silently ignore any exception
                    # raised here, but for now I am going to print it to stderr.
                    msg = f"Exception '{e}' raised while stopping ECS task"
                    print(msg, file=sys.stderr)

    def createTaskDef(self):
        """
        If requested to do so, create a task definition for the worker tasks
        """
        taskDef_kwArgs = self.extraParams.get('register_task_definition')
        if taskDef_kwArgs is not None:
            taskDefResponse = self.ecsClient.register_task_definition(**taskDef_kwArgs)
            self.taskDefArn = taskDefResponse['taskDefinition']['taskDefinitionArn']
            self.createdTaskDef = True

    def checkTaskErrors(self):
        """
        Check for errors in any of the worker tasks, and report to stderr.
        """
        if self.taskArnList is None:
            return

        normalStopCodes = set(['EssentialContainerExited'])
        numTasks = len(self.taskArnList)
        # The describe_tasks call will only take this many at a time, so we
        # have to page through.
        TASKS_PER_PAGE = 100
        i = 0
        failures = []
        exitCodeList = []
        stoppedList = []
        while i < numTasks:
            j = i + TASKS_PER_PAGE
            descr = self.ecsClient.describe_tasks(cluster=self.clusterName,
                tasks=self.taskArnList[i:j])
            failures.extend(descr['failures'])
            for t in descr['tasks']:
                stoppedList.append((t.get('stopCode'), t.get('stoppedReason')))
            # Grab all the container exit codes/reasons. Note that we
            # know we have only one container per task.
            ctrDescrList = [t['containers'][0] for t in descr['tasks']]
            for c in ctrDescrList:
                if 'exitCode' in c:
                    exitCode = c['exitCode']
                    if exitCode != 0:
                        reason = c.get('reason', "UnknownReason")
                        exitCodeList.append((exitCode, reason))
            i = j

        for f in failures:
            print("Failure in ECS task:", f.get('reason'), file=sys.stderr)
            print("    ", f.get('details'), file=sys.stderr)
        for (stopCode, stoppedReason) in stoppedList:
            if stopCode is not None or stoppedReason is not None:
                if stopCode not in normalStopCodes:
                    msg = f"Task stopped: {stopCode}. Reason: {stoppedReason}"
                    print(msg, file=sys.stderr)
        for (exitCode, reason) in exitCodeList:
            if exitCode != 0:
                print("Exit code {} from ECS task container: {}".format(
                    exitCode, reason), file=sys.stderr)


class _NetworkDataChannel:
    """
    A network-visible channel to serve out all the required information to
    a group of ecscall workers.

    The channel has several major attributes.

        userFunc : function
            The user function to call (stored in cloudpickle-ed form)
        argsQue : Queue
            Each element of this queue is a tuple (i, argsTuple), where i is
            the index number and argsTuple is a tuple of arguments for the
            user function
        returnValQue : Queue
            As each call completes, its return value is placed in this queue,
            along with its index, as a tuple (i, returnValue)
        forceExit : Event
            If set, this signals that workers should exit
            as soon as possible
        exceptionQue : Queue
            Any exceptions raised in the worker are caught and put
            into this queue, to be dealt with in the main thread.
        workerBarrier : Barrier
            For the relevant compute worker kinds, all
            workers will wait at this barrier, as will the main thread,
            so that no processing starts until all compute workers are
            ready to work.

    If the constructor is given these major objects as arguments, then this
    is the server of these objects, and they are served to the network on
    a selected port number. The address of this server is available on the
    instance as hostname, portnum and authkey attributes. The server will
    create its own thread in which to run.

    A client instance can be created by giving the constructor the hostname,
    port number and authkey (obtained from the server object). This will then
    connect to the server, and make available the data attributes as given.

    The server must be shut down correctly, and so the shutdown() method
    should always be called explicitly.

    """
    def __init__(self, userFunc=None, argsQue=None, returnValQue=None,
            forceExit=None, exceptionQue=None, workerBarrier=None,
            hostname=None, portnum=None, authkey=None):
        class DataChannelMgr(BaseManager):
            pass

        if None not in (userFunc, argsQue, returnValQue):
            self.hostname = socket.gethostname()
            # Authkey is a big long random bytes string. Make one which is
            # also printable ascii.
            self.authkey = secrets.token_hex()

            self.userFunc = cloudpickle.dumps(userFunc)
            self.argsQue = argsQue
            self.returnValQue = returnValQue
            self.forceExit = forceExit
            self.exceptionQue = exceptionQue
            self.workerBarrier = workerBarrier

            DataChannelMgr.register("get_userfunc",
                callable=lambda: self.userFunc)
            DataChannelMgr.register("get_argsque",
                callable=lambda: self.argsQue)
            DataChannelMgr.register("get_returnvalque",
                callable=lambda: self.returnValQue)
            DataChannelMgr.register("get_forceexit",
                callable=lambda: self.forceExit)
            DataChannelMgr.register("get_exceptionque",
                callable=lambda: self.exceptionQue)
            DataChannelMgr.register("get_workerbarrier",
                callable=lambda: self.workerBarrier)

            self.mgr = DataChannelMgr(address=(self.hostname, 0),
                                     authkey=bytes(self.authkey, 'utf-8'))

            self.server = self.mgr.get_server()
            self.portnum = self.server.address[1]
            self.threadPool = futures.ThreadPoolExecutor(max_workers=1)
            self.serverThread = self.threadPool.submit(
                self.server.serve_forever)
        elif None not in (hostname, portnum, authkey):
            DataChannelMgr.register("get_userfunc")
            DataChannelMgr.register("get_argsque")
            DataChannelMgr.register("get_returnvalque")
            DataChannelMgr.register("get_forceexit")
            DataChannelMgr.register("get_exceptionque")
            DataChannelMgr.register("get_workerbarrier")

            self.mgr = DataChannelMgr(address=(hostname, portnum),
                                     authkey=authkey)
            self.hostname = hostname
            self.portnum = portnum
            self.authkey = authkey
            self.mgr.connect()

            # Get the proxy objects.
            self.userFunc = cloudpickle.loads(eval(str(
                self.mgr.get_userfunc())))
            self.argsQue = self.mgr.get_argsque()
            self.returnValQue = self.mgr.get_returnvalque()
            self.forceExit = self.mgr.get_forceexit()
            self.exceptionQue = self.mgr.get_exceptionque()
            self.workerBarrier = self.mgr.get_workerbarrier()
        else:
            msg = ("Must supply either (userFunc, argsQue, etc.)" +
                   " or ALL of (hostname, portnum and authkey)")
            raise ValueError(msg)

    def shutdown(self):
        """
        Shut down the NetworkDataChannel in the right order. This should always
        be called explicitly by the creator, when it is no longer
        needed. If left to the garbage collector and/or the interpreter
        exit code, things are shut down in the wrong order, and the
        interpreter hangs on exit.

        I have tried __del__, also weakref.finalize and atexit.register,
        and none of them avoid these problems. So, just make sure you
        call shutdown explicitly, in the process which created the
        NetworkDataChannel.

        The client processes don't seem to care, presumably because they
        are not running the server thread. Calling shutdown on the client
        does nothing.

        """
        if hasattr(self, 'server'):
            self.server.stop_event.set()
            if self.workerBarrier is not None:
                self.workerBarrier.abort()
            futures.wait([self.serverThread])
            self.threadPool.shutdown()

    def addressStr(self):
        """
        Return a single string encoding the network address of this channel
        """
        s = "{},{},{}".format(self.hostname, self.portnum, self.authkey)
        return s


class EcsCallError(Exception):
    pass
