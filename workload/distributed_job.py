import time
import logging
import redis

from multiprocessing.pool import ThreadPool
from .utils import (
    LOG_TRIM,
    MAX_RETRY_SLEEP,
    DEFAULT_CHUNK_SIZE,
    parse_int,
    chunk_workload,
)


LUA_SPOPMOVE = """
redis.replicate_commands()
local v = redis.call("SPOP", KEYS[1])
if v then
    redis.call("SADD", KEYS[2], v)
end
return v
"""

LUA_SCARD2 = """
local v1 = redis.call("SCARD", KEYS[1])
local v2 = redis.call("SCARD", KEYS[2])
return {v1, v2}
"""


class DistributedJobController:
    __slots__ = [
        '__redis_client',

        '__key_result',
        '__key_workload',
        '__key_nack',
        '__key_fanout',
    ]

    def __init__(
        self, redis_client,
        key_result, key_workload, key_fanout, key_nack
    ):
        self.__redis_client = redis_client

        self.__key_result = key_result
        self.__key_workload = key_workload
        self.__key_fanout = key_fanout
        self.__key_nack = key_nack

    def result(self, *results):
        if results:
            self.__redis_client.sadd(self.__key_result, *results)

    def fanout(self, workload):
        (
            self.__redis_client
                .pipeline()
                .incr(self.__key_fanout)
                .sadd(self.__key_workload, *workload)
                .execute()
        )

    def error(self, reason):
        return Exception(reason)


class DistributedJob:
    __slots__ = [
        'logger',
        '__redis_client',
        '__name',
        '__callback',
        '__controller',
        '__run',

        '__key_result',
        '__key_workload',
        '__key_nack',
        '__key_tasks',
        '__key_start_time',
        '__key_end_time',
        '__key_workers',
        '__key_success',
        '__key_error',
        '__key_fanout',
        '__func_spopmove',
        '__func_scard2',
    ]

    def __init__(self, name, callback, redis_pool):
        self.logger = logging.getLogger('distributed')
        self.__redis_client = redis.StrictRedis(connection_pool=redis_pool)
        self.__name = name
        self.__callback = callback
        self.__run = True

        self.__key_result = '{}.result'.format(name)
        self.__key_workload = '{}.workload'.format(name)
        self.__key_nack = '{}.nack'.format(name)
        self.__key_start_time = '{}.start_time'.format(name)
        self.__key_end_time = '{}.end_time'.format(name)
        self.__key_workers = '{}.workers'.format(name)
        self.__key_success = '{}.success'.format(name)
        self.__key_error = '{}.error'.format(name)
        self.__key_fanout = '{}.fanout'.format(name)

        self.__func_spopmove = self.__redis_client.register_script(LUA_SPOPMOVE)
        self.__func_scard2 = self.__redis_client.register_script(LUA_SCARD2)

        self.__controller = DistributedJobController(
            redis_client=self.__redis_client,
            key_result=self.__key_result,
            key_workload=self.__key_workload,
            key_fanout=self.__key_fanout,
            key_nack=self.__key_nack
        )

    @property
    def name(self):
        return self.__name

    @property
    def results(self):
        yield from self.__redis_client.sscan_iter(self.__key_result)

    def callback(self, args):
        """
        Single threaded function that invokes job processing
        """
        workload = self.__func_spopmove(keys=[self.__key_workload, self.__key_nack])
        if workload is None:
            # no task currently in queue
            return

        workload = workload.decode('utf-8')
        self.logger.debug('{}: processing job {}...'.format(self.__name, workload[:LOG_TRIM]))

        try:
            self.__callback(self.__controller, workload, *args)
        except Exception as e:
            self.logger.error(e)
            # Error occurred, remove task from nack and add it back to workload
            (
                self.__redis_client.pipeline()
                                   .incr(self.__key_error)
                                   .sadd(self.__key_workload, workload)
                                   .srem(self.__key_nack, workload)
                                   .execute()
            )
        else:
            # No errors happend, ack task, update counters
            (
                self.__redis_client.pipeline()
                                   .incr(self.__key_success)
                                   .srem(self.__key_nack, workload)
                                   .execute()
            )

    def describe(self):
        """
        Get job statistics
        """
        in_progress = sum(self.__func_scard2(keys=[self.__key_workload, self.__key_nack])) > 0
        start_time = parse_int(self.__redis_client.get(self.__key_start_time))
        end_time = parse_int(self.__redis_client.get(self.__key_end_time))
        if in_progress:
            end_time = int(time.time())

        return {
            'workers': parse_int(self.__redis_client.get(self.__key_workers)),
            'in_progress': in_progress,
            'duration': end_time - start_time,
            'results': parse_int(self.__redis_client.scard(self.__key_result)),
            'workload': parse_int(self.__redis_client.scard(self.__key_workload)),
            'errors': parse_int(self.__redis_client.get(self.__key_error)),
            'tech_name': self.__name,
        }

    def distribute(self, workload, chunk_len=DEFAULT_CHUNK_SIZE):
        """
        Start distributed job
        """
        if chunk_len:
            workload = chunk_workload(workload, size=DEFAULT_CHUNK_SIZE)
        else:
            workload = (workload,)

        pipeline = self.__redis_client.pipeline()
        (
            pipeline
                .delete(self.__key_result)
                .delete(self.__key_workload)
                .delete(self.__key_nack)
                .set(self.__key_start_time, int(time.time()))
                .set(self.__key_end_time, 0)
                .set(self.__key_workers, 0)
                .set(self.__key_success, 0)
                .set(self.__key_error, 0)
                .set(self.__key_fanout, 0)
        )
        for chunk in workload:
            pipeline.sadd(self.__key_workload, *chunk)
            break

        pipeline.execute()

        for chunk in workload:
            self.__redis_client.sadd(self.__key_workload, *chunk)

    def cancel(self):
        (
            self.__redis_client
                .pipeline()
                .delete(self.__key_workload)
                .set(self.__key_end_time, int(time.time()))
                .execute()
        )

    def wait_results(self):
        while True:
            if sum(self.__func_scard2(keys=[self.__key_workload, self.__key_nack])) == 0:
                break
            time.sleep(0.001)

    def start_bulk(self, concurrency=1, pool_args=()):
        concurrency, pool_args = self._normalize_pool_args(concurrency, pool_args)
        pool = ThreadPool(processes=concurrency)
        self._run_forever(task=lambda: pool.map(self.callback, pool_args))

    def start_threaded(self, concurrency=1, pool_args=()):
        concurrency, pool_args = self._normalize_pool_args(concurrency, pool_args)
        pool = ThreadPool(processes=concurrency)

        pool.map(
            lambda args: self.start_single(*args),
            pool_args
        )

    def start_single(self, *args):
        self._run_forever(task=lambda: self.callback(args))

    def _normalize_pool_args(self, concurrency=1, pool_args=()):
        if not pool_args:
            pool_args = [()] * concurrency
        concurrency = len(pool_args)
        return concurrency, pool_args

    def _run_forever(self, task):
        exception_tries = 0

        self.__run = True
        while self.__run:
            try:
                if not parse_int(self.__redis_client.scard(self.__key_workload)):
                    time.sleep(1)
                    continue

                try:
                    self.__redis_client.incr(self.__key_workers)

                    while parse_int(self.__redis_client.scard(self.__key_workload)):
                        task()
                        time.sleep(0.001)

                    self.logger.info('{}: finished processing'.format(self.__name))
                finally:
                    self.__redis_client.decr(self.__key_workers)
                    self.__redis_client.set(self.__key_end_time, int(time.time()))
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


def distributed(name, redis_pool):
    def decorator(func):
        return DistributedJob(name, func, redis_pool=redis_pool)
    return decorator


class DistributedPool:
    def __init__(self, *tasks):
        self.__tasks = {}
        for index, task in enumerate(tasks):
            if not isinstance(task, DistributedJob):
                raise Exception('Task {} is not distributed'.format(index))
            if task.name in self.__tasks:
                raise Exception('Task {} with name {} already exist'.format(index, task.name))
            self.__tasks[task.name] = task

    def start(self, task_name, concurrency, pool_args):
        self.__tasks[task_name].start_processing(concurrency, pool_args)
