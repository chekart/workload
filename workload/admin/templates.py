import os
from jinja2 import Template


TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), 'jinja')
TEMPLATE_CACHE = {}


def get_template(name):
    if name in TEMPLATE_CACHE:
        return TEMPLATE_CACHE[name]
    return load_template(name)


def load_template(name):
    assert not name.startswith('/')

    with open(os.path.join(TEMPLATES_DIR, name), 'r') as f:
        content = ''.join(f.readlines())

    template = Template(content, autoescape=True)
    TEMPLATE_CACHE[name] = template

    return template


def render_cached_template(name, context):
    template = get_template(name)
    return template.render(context)


def render_template(name, context):
    template = load_template(name)
    return template.render(context)
