"""
Usage:

  cycle([
    (cycle.at(hour=1), job1),
    (cycle.interval(minute=5), job2),
  ])

"""
import logging

from time import sleep
from datetime import datetime, timedelta


LOG = logging.getLogger('workload.cycle')


class At:
    __slots__ = [
        'hour',
        'minute',
        'second',
    ]

    def __init__(self, hour=0, minute=0, second=0):
        self.hour = hour
        self.minute = minute
        self.second = second

    def schedule(self, prev, now):
        next_ = now + timedelta(days=1)
        next_.replace(
            hour=self.hour,
            minute=self.minute,
            second=self.minute
        )
        return next_


class Interval:
    __slots__ = [
        'hours',
        'minutes',
        'seconds',
    ]

    def __init__(self, hours=0, minutes=0, seconds=0):
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds

    def schedule(self, prev, now):
        next_ = prev + timedelta(
            hours=self.hours,
            minutes=self.minutes,
            seconds=self.seconds
        )
        return next_


class Task:
    __slots__ = [
        'at', 'when', 'job'
    ]

    def __init__(self, at, when, job):
        self.at = at
        self.when = when
        self.job = job


def cycle(sheduled_jobs):
    """
    Start scheduled job worker. The worker will push deferred tasks to
    redis queue
    """
    queue = []
    now = datetime.utcnow()
    for when, job in sheduled_jobs:
        if not hasattr(job, 'defer'):
            raise RuntimeError('Job should have defer method')

        queue.append(Task(at=when.schedule(now, now), when=when, job=job))

    while True:
        now = datetime.utcnow()
        for task in queue:
            if task.at > now:
                continue

            task.job.defer()
            task.at = task.when.schedule(prev=task.at, now=now)

        sleep(0.001)


cycle.at = At
cycle.interval = Interval
