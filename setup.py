#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name='cpg-utils',
    # This tag is automatically updated by bumpversion
    version='4.18.2',
    description='Library of convenience functions specific to the CPG',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/populationgenomics/cpg-utils',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'boto3==1.28.56',
        'botocore==1.31.56',
        'google-auth>=1.27.0',
        'google-cloud-secret-manager',
        'cloudpathlib[all]',
        'toml',
        'frozendict',
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-mock',
        ],
    },
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
