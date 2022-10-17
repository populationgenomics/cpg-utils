#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name='cpg-utils',
    # This tag is automatically updated by bumpversion
    version='4.8.18',
    description='Library of convenience functions specific to the CPG',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url=f'https://github.com/populationgenomics/cpg-utils',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'google-auth',
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
        'workflows': [
            'hail',
            'networkx',
            'sample-metadata',
            'analysis-runner',
            'gnomad',
            'pandas',
            'peddy',
            'fsspec',
            'slack_sdk',
            'elasticsearch == 8.*',
            'coloredlogs',
            'bokeh',
            'numpy',
            'click',
            'elasticsearch == 8.*',
            'slack_sdk',
        ],
    },
    package_data={
        'cpg_utils': ['config-template.toml'],
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
