import re
import json
import base64

import falcon
import redis

from collections import OrderedDict

from workload.deferred_job import DeferredJob
from workload.distributed_job import DistributedJob

from .templates import render_cached_template, render_template


LEADING_SPACES = re.compile(r'$s+?')


class IndexResource:
    def __init__(self, title, url_prefix, jobs, show_status, debug):
        self.__title = title
        self.__url_prefix = url_prefix
        self.__show_status = show_status
        self.__jobs = jobs
        self.__render_template = render_template if debug else render_cached_template

    def on_get(self, req, resp):
        resp.content_type = 'text/html'
        jobs = []
        statuses = StatusResource.collect_jobs_status(self.__jobs)
        for name, info in self.__jobs.items():
            jobs.append({
                'name': name,
                'label_prefix': info['job_module'],
                'label': info['job_func'],
                'description': self.format_description(info['description']),
                'type': info['type'],
                'status': statuses.get(name, {}),
                'can_start': info['start'] is not None,
            })

        resp.body = self.__render_template('index.html', {
            'app_title': self.__title,
            'url_prefix': self.__url_prefix,
            'app_show_status': self.__show_status,
            'jobs': jobs,
        })

    def format_description(self, desc):
        if not desc:
            return desc

        desc = desc.splitlines()
        result = []

        base_offset = None
        for line in desc:
            if not line.strip():
                if base_offset is not None:
                    result.append('')
                continue

            offset = len(line) - len(line.lstrip(' '))
            if base_offset is None:
                base_offset = offset

            if line.startswith(' ' * base_offset):
                line = line[base_offset:]

            result.append(line)

        return '\n'.join(result)


class StatusResource:
    def __init__(self, jobs):
        self.__jobs = jobs

    def on_get(self, req, resp):
        resp.content_type = 'application/json'
        resp.body = json.dumps(self.collect_jobs_status(self.__jobs))

    @classmethod
    def collect_jobs_status(cls, jobs):
        return {
            name: info['job'].describe() for name, info in jobs.items()
        }


class TaskActionResource:
    def __init__(self, jobs):
        self.__jobs = jobs

    def on_post(self, req, resp):
        name = req.params.get('job')
        action = req.params.get('action')

        if name not in self.__jobs:
            raise falcon.HTTPNotFound()

        info = self.__jobs[name]

        job = info['job']
        if action == 'start':
            start = info['start']
            if start is None:
                raise falcon.HTTPNotFound()
            start(job)
        elif action == 'stop':
            job.cancel()

        resp.body = json.dumps({
            'status': 'ok',
        })


class RedisStatusResource:
    def __init__(self, redis_pool):
        self.client = redis.StrictRedis(connection_pool=redis_pool)

    def on_get(self, req, resp):
        info = self.client.info()

        try:
            used_memory = (info['used_memory'] / info['total_system_memory'])
        except ZeroDivisionError:
            used_memory = 0

        resp.content_type = 'application/json'
        resp.body = json.dumps({
            'redis_version': info['redis_version'],
            'os': info['os'],
            'uptime_in_days': info['uptime_in_days'],
            'used_memory': info['used_memory_human'],
            'total_memory': info['total_system_memory_human'],
            'used_memory_percent': int(used_memory),
            'last_save': info['rdb_last_save_time'],
        })


class AuthMiddleware:
    def __init__(self, username, password):
        self.__username = username
        self.__password = password

    def __auth_failed(self):
        return falcon.HTTPUnauthorized(
            'Login required',
            challenges=['Basic realm="User Visible Realm" charset="UTF-8"']
        )

    def process_request(self, req, resp):
        token = req.get_header('Authorization')
        if not token or not token.startswith('Basic '):
            raise self.__auth_failed()

        try:
            token = base64.b64decode(token[len('Basic '):]).decode('utf-8')
            token = token.split(':')
            username = token[0]
            password = ':'.join(token[1:])
        except Exception:
            raise self.__auth_failed()

        if not (
            username == self.__username and
            password == self.__password
        ):
            raise self.__auth_failed()


def __get_job_name(job):
    return '{}.{}'.format(job.action.__module__, job.action.__name__)


def start_defer(job):
    job.defer()


START_DEFER = start_defer


def create_admin_app(prefix, jobs, title='Workload Admin', credentials=None, debug=False, redis_pool=None):
    """
    Create WSGI application to administrate workload jobs
    :param prefix: url prefix for all routes, for example '/' or '/admin'
    :param jobs: iterable of jobs (if no start is allowed) or tuple (job, callable to start job)
        it is possible to use START_DEFER as a shortcut for job without arguments
    :param title: brand title to display
    :param credentials: tuple of (user, pass) or None if no auth needed
    :param debug: turn of debug mode for template rendering
    :param redis_pool: redis pool to track redis server information
    :return: WSGI app
    """
    tasks = OrderedDict()

    for job in jobs:
        start_func = None
        if isinstance(job, (tuple, list)):
            job, start_func = job

        assert isinstance(job, (DeferredJob, DistributedJob))
        name = __get_job_name(job)
        assert name not in tasks

        tasks[name] = {
            'job': job,
            'job_module': str(job.action.__module__),
            'job_func': str(job.action.__name__),
            'name': name,
            'description': job.action.__doc__,
            'start': start_func,
            'type': 'deferred' if isinstance(job, DeferredJob) else 'distributed'
        }

    middleware = []
    if credentials:
        username, password = credentials
        middleware.append(AuthMiddleware(username=username, password=password))

    app = falcon.API(middleware=middleware)
    app.req_options.auto_parse_form_urlencoded = True
    app.add_route('{}'.format(prefix), IndexResource(title, prefix, tasks,
                                                     show_status=redis_pool is not None, debug=debug))
    app.add_route('{}/status'.format(prefix), StatusResource(tasks))
    app.add_route('{}/actions'.format(prefix), TaskActionResource(tasks))
    if redis_pool:
        app.add_route('{}/rstatus'.format(prefix), RedisStatusResource(redis_pool))

    return app
