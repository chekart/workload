import json
import time
import logging
import redis

from .utils import LOG_TRIM, MAX_RETRY_SLEEP


class DeferredJob:
    __slots__ = [
        'logger',
        '__redis_client',
        '__name',
        '__callback',
        '__run',

        '__key_queue',
    ]

    def __init__(self, name, callback, redis_pool):
        self.logger = logging.getLogger('deferred')
        self.__name = name
        self.__callback = callback
        self.__redis_client = redis.StrictRedis(connection_pool=redis_pool)
        self.__run = True

        self.__key_queue = '{}.queue'.format(self.__name)

    @property
    def name(self):
        return self.__name

    def defer(self, workload=None):
        self.__redis_client.rpush(self.__key_queue, json.dumps(workload))

    def start_processing(self):
        self.__run = True
        exception_tries = 0

        while self.__run:
            try:
                task = self.__redis_client.lpop(self.__key_queue)
                if task is None:
                    time.sleep(1)
                    continue

                task = task.decode('utf-8')
                task_log = task[:LOG_TRIM]

                try:
                    self.logger.info('{}: started processing {}'.format(self.__name, task_log))
                    self.__callback(json.loads(task))
                    self.logger.info('{}: finished processing {}'.format(self.__name, task_log))
                except Exception as e:
                    self.logger.error('{}: failed to process {}, reason {}'.format(
                        self.__name, task_log, e
                    ))

                time.sleep(0.001)
            except Exception as e:
                exception_tries += 1
                self.logger.error('{}: exception during processing loop. Increasing wait time. {}'.format(
                    self.__name, e
                ))
                time.sleep(min(2 ** exception_tries, MAX_RETRY_SLEEP))
            else:
                exception_tries = 0

    def stop_processing(self):
        self.__run = False


def deferred(name, redis_pool):
    def decorator(func):
        return DeferredJob(name, func, redis_pool=redis_pool)
    return decorator


class DeferredPool:
    def __init__(self, *tasks):
        self.__tasks = {}
        for index, task in enumerate(tasks):
            if not isinstance(task, DeferredJob):
                raise Exception('Task {} is not deferred'.format(index))
            if task.name in self.__tasks:
                raise Exception('Task {} with name {} already exist'.format(index, task.name))
            self.__tasks[task.name] = task

    def start(self, task_name):
        self.__tasks[task_name].start_processing()
