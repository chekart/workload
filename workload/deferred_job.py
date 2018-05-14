import json
import time
import logging
import redis

from .utils import LOG_TRIM, MAX_RETRY_SLEEP, parse_int


LOG = logging.getLogger('workload.deferred')


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
        self.__name = name
        self.__callback = callback
        self.__redis_client = redis.StrictRedis(connection_pool=redis_pool)
        self.__run = True

        self.__key_queue = '{}.queue'.format(self.__name)

    @property
    def name(self):
        return self.__name

    @property
    def action(self):
        return self.__callback

    def describe(self):
        """
        Get job statistics
        """
        return {
            'queue': parse_int(self.__redis_client.llen(self.__key_queue)),
            'type': 'deferred',
            'tech_name': self.__name,
        }

    def defer(self, workload=None):
        self.__redis_client.rpush(self.__key_queue, json.dumps(workload))

    def cancel(self):
        self.__redis_client.delete(self.__key_queue)

    def start_processing(self):
        self.__run = True
        exception_tries = 0

        while self.__run:
            try:
                result = self.process_one()
                if result is None:
                    time.sleep(1)
                    continue

                time.sleep(0.001)
            except Exception as e:
                exception_tries += 1
                LOG.error('{}: exception during processing loop. Increasing wait time. {}'.format(
                    self.__name, e
                ))
                time.sleep(min(2 ** exception_tries, MAX_RETRY_SLEEP))
            else:
                exception_tries = 0

    def process_one(self):
        """
        Process one task from top of the queue.
        :raises: Redis errors, decoding errors
        :return: None if there is no tasks queued, True if task successfully processed, False otherwise
        """
        task = self.__redis_client.lpop(self.__key_queue)
        if task is None:
            return None

        task = task.decode('utf-8')
        task_log = task[:LOG_TRIM]

        try:
            LOG.info('{}: started processing {}'.format(self.__name, task_log))
            self.__callback(json.loads(task))
            LOG.info('{}: finished processing {}'.format(self.__name, task_log))
        except Exception as e:
            LOG.error('{}: failed to process {}, reason {}'.format(
                self.__name, task_log, e
            ))
            return False

        return True

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

    def start_all(self):
        while True:
            for task_name, task in self.__tasks.items():
                try:
                    task.process_one()
                except Exception as e:
                    LOG.error('{}: failed to process, reason {}'.format(task_name, e))

            time.sleep(0.001)

