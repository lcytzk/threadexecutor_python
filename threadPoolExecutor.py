import threading, Queue
import logging
from tpeException import *

logger = logging.getLogger()

class ExecutorTask(object):

    def run(self):
        pass

class Executor(threading.Thread):

    fingerprint = 'Executor'

    def __init__(self, threadPool, NO):
        threading.Thread.__init__(self)
        self.threadPool = threadPool
        self.closed = False
        self.NO = NO
        self.task = None

    def run(self):
        self.getNextTask()
        while not self.closed and self.task != None:
            try:
                self.task.run()
            except:
                logger.error('Execute task error.')
            self.getNextTask()
        self.finish()

    def finish(self):
        logger.info('A executor finished and ready to exit')

    def getNextTask(self):
        self.task = self.threadPool.getNextTask()

    def stop(self):
        self.closed = True

    def __hash__(self):
        return hash((Executor.fingerprint, self.NO))

class ThreadPoolExecutor(object):

    def __init__(self, taskNum = 0, executorNum = 10, timeout = 5):
        self.taskQueue = Queue.Queue(taskNum)
        self.executors = set()
        self.taskNum = taskNum
        self.executorNum = executorNum
        self.closed = False
        self.forceClosed = False
        self.timeout = timeout
        self.executorLock = threading.Lock()

    def submit(self, task):
        if self.closed:
            logger.info("Thread exector has been closed.")
            raise TPEClosedException()
        if task == None:
            raise TPENoneException()
        if not isinstance(task, ExecutorTask):
            raise TPEMistypeException()
        self.accept(task)
        return True

    def accept(self, task):
        self.taskQueue.put(task)
        if len(self.executors) < self.executorNum:
            self.tryCreateNewExecutor()

    def tryCreateNewExecutor(self):
        self.executorLock.acquire()
        workNO = len(self.executors)
        if workNO < self.executorNum:
            executor = Executor(self, workNO)
            self.executors.add(executor)
            executor.start()
        self.executorLock.release()

    def getNextTask(self):
        if self.forceClosed:
            return None
        try:
            return self.taskQueue.get(timeout = self.timeout)
        except:
            return None

    def forceStop(self):
        self.fourceClosed = True
        self.stop()

    def stop(self):
        self.closed = True
        for executor in self.executors:
            executor.stop()

    def wait(self):
        logger.info('Waiting for all task to finish.')
        for executor in self.executors:
            executor.join()
