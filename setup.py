#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    long_description = f.read()

setup(
    name='cpg-utils',
    # This tag is automatically updated by bumpversion
    version='5.2.0',
    description='Library of convenience functions specific to the CPG',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/populationgenomics/cpg-utils',
    license='MIT',
    packages=find_packages(),
    package_data={
        'cpg_utils': ['py.typed'],
    },
    install_requires=[
        'boto3>=1.28.56',
        'botocore>=1.31.56',
        'cloudpathlib[all]',
        'frozendict',
        'google-auth>=1.27.0',
        'google-cloud-secret-manager',
        'requests',
        'tabulate',
        'toml',
        'deprecated',
    ],
    keywords='bioinformatics',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
