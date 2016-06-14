#!/usr/bin/env python

import time

from threadPoolExecutor import *

class MyTask(ExecutorTask):

    def __init__(self, name):
        self.name = name

    def run(self):
        print self.name

def test():
    threadpool = ThreadPoolExecutor(timeout=1)
    for i in range(1000):
        threadpool.submit(MyTask('Task %d' % i))
    threadpool.wait()



if __name__ == '__main__':
    test()
