from setuptools import setup

setup(
    name='sequila.py',
    version='0.1.1',
    packages=['sequila'],
    install_requires=[
        'typeguard==2.5.0',
        'pyspark==2.4.3',
        'findspark'
    ],
    author='biodatageeks.org',
    description='A SQL-based solution for large-scale genomic analysis',
    long_description=open('README.rst').read(),
    long_description_content_type='text/x-rst',
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
    ],
    url='https://github.com/ZSI-Bio/bdg-sequila'
)