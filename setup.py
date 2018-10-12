import sys
from distutils.core import setup

if sys.version_info < (3, 0):
    raise NotImplementedError("Sorry only Python 3+ is supported")


with open('README.md', 'rt') as f:
    long_description = f.read()


setup(
    name='workload',
    packages=['workload'],
    version='0.1',
    description='A simple task distribution library',
    long_description=long_description,
    license='MIT',
    author='chekart',
    url='https://github.com/chekart/workload',
    keywords=['task', 'distribution',],
    install_requires=[
        'redis',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ]
)