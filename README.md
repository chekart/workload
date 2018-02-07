# Workload

Simple, clean, easy python task distribution
No magic, no configuration, no broker abstraction

The library uses Redis as broker

Usage:

### Create distributed job

```
import redis
from workload import distributed

REDIS_POOL = redis.ConnectionPool()

@distributed('worker', redis_pool=REDIS_POOL)
def worker(job, country):
    # do stuff
    job.result('result') # add unique result
    job.fanout(['task3', 'task4']) # schedule another tasks if needed
    job.error('oops') # raise and catch exception
```

### Add workload to do

```
from jobs import worker

worker.distribute(['task1', 'task2'])
worker.wait_results()
```

### Start worker

```
from jobs import worker

worker.start_processing(concurrency=10)
```