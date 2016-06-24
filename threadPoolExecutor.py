import threading, Queue
import logging
from abc import ABCMeta, abstractmethod
from tpeException import *

logging.basicConfig()
logger = logging.getLogger('ThreadPool')
logger.setLevel(logging.INFO)

'''
    Abstract executor task.
'''
class ExecutorTask:
    __metaclass__ = ABCMeta

    @abstractmethod
    def run(self): pass

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
        '''
            There are two situations which will exit this executor.
                1. When executor has been closed.
                2. When there is no task in queue
        '''
        while not self.closed and self.task != None:
            try:
                self.task.run()
            except:
                logger.error('Execute task error.')
            self.getNextTask()
        self.finish()

    def finish(self):
        self.threadPool.finish(self)
        logger.info('A executor finished and ready to exit, work number is %d', self.NO)

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
        self.workerNumber = 0

    '''
        Submit a task, return True if success, raise an exception otherwise.
    '''
    def submit(self, task):
        if self.closed:
            logger.info("Thread executor has been closed.")
            raise TPEClosedException()
        if task == None:
            raise TPENoneException()
        if not isinstance(task, ExecutorTask):
            raise TPEMistypeException()
        self.__accept(task)
        return True

    def __accept(self, task):
        # Here can be optimized, if the number of executors are less than maximum,
        # the new task can be executed immediately.
        self.taskQueue.put(task)
        if len(self.executors) < self.executorNum:
            self.tryCreateNewExecutor()

    def tryCreateNewExecutor(self):
        try:
            self.executorLock.acquire()
            if len(self.executors) < self.executorNum:
                executor = Executor(self, self.workerNumber)
                self.workerNumber += 1
                self.executors.add(executor)
                executor.start()
        finally:
            self.executorLock.release()

    '''
        If thread pool has been force stop, get next task will return None immediately,
        otherwise, return a task.
        If queue is empty and after timeout, it will return None.
    '''
    def getNextTask(self):
        if self.forceClosed:
            return None
        try:
            return self.taskQueue.get(timeout = self.timeout)
        except:
            return None

    '''
        When thread pool has been force stop, running tasks will be executed properly,
        but tasks waiting in queue will be abandoned.

        Persisting method can be added if necessary.
    '''
    def forceStop(self):
        self.fourceClosed = True
        self.stop()

    '''
        When thread pool has been stop, it will no more receive new tasks,
        remaining tasks will be executed then thread pool will exit.
    '''
    def stop(self):
        self.closed = True
        for executor in self.executors:
            executor.stop()

    '''
        Wait until all executor exit.
    '''
    def wait(self):
        logger.info('Waiting for all task to finish.')
        for executor in list(self.executors):
            executor.join()
        logger.info("All tasks have finished.")

    def finish(self, task):
        try:
            self.executors.remove(task)
        except:
            logger.warn("No NO:%d exist." % task.NO)
