import threading, Queue
import logging

logger = logging.getLogger('ThreadExector')

class ExectorTask(object):

    def run():
        pass


class Executor(threading.Thread):

    def __init__(self, threadPool):
        threading.Thread.__init___(self)
        self.threadPool = threadPool
        self.closed = False

    def run(self):
        while not self.closed and self.task != None:
            try:
                self.task.run()
            except:
                logger.error('Execute task error.')
        self.finish()

    def finish(self):
        logger.info('A executor finished adn ready to exit')

    def getNextTask(self):
        self.task = self.threadPool.getNextTask()

    def stop(self):
        self.closed = True

class TPEException(Exception):
    pass

class TPEClosedException(ThreadExectorException):
    pass

class TPEMistypeException(ThreadExectorException):
    pass

class TPENoneException(ThreadExectorException):
    pass

class ThreadPoolExector(object):

    def __init__(self, taskNum = 10, timeout = 5):
        self.taskQueue = Queue.Queue()
        self.executors = []
        self.taskNum = taskNum
        self.closed = False
        self.forceClosed = False
        self.timeout = timeout

    def submit(self, task):
        if self.closed:
            logger.info("Thread exector has been closed.")
            raise TPEClosedException()
        if task == None:
            raise TPENoneException()
        if not isinstance(task, ExectorTask):
            raise TPEMistypeException()
        self.taskQueue.put(task)
        return True

    def getNextTask(self):
        if self.fourceClosed:
            return None
        try:
            return self.taskQueue.get(timeout = self.timeout)
        except:
            return None

    def forceStop(self):
        self.closed = True
        self.fourceClosed = True

    def stop(self):
        self.closed = True

    def wait(self):
        logger.info('Waiting for all task to finish.')
        for executor in self.executors:
            executor.join()
