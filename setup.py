#!/usr/bin/env python

import setuptools

setuptools.setup(
    name='cpg-utils',
    # This tag is automatically updated by bumpversion
    version='2.1.1',
    description='Library of convenience functions specific to the CPG',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url=f'https://github.com/populationgenomics/cpg-utils',
    license='MIT',
    packages=['cpg_utils'],
    include_package_data=True,
    keywords='bioinformatics',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
