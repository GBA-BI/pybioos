from setuptools import find_packages, setup

from bioos.__about__ import __version__

setup(
    name="pybioos",
    version=__version__,
    keywords=["pip", "bioos"],
    description="BioOs SDK for Python",
    license="MIT Licence",
    url="https://github.com/GBA-BI/pybioos",
    author="Jilong Liu",
    author_email="liu_jilong@gzlab.ac.cn",
    packages=find_packages(),
    platforms="any",
    install_requires=[
        "volcengine>=1.0.61",
        "tabulate>=0.8.10",
        "click>=8.0.0",
        "pandas>=1.3.0",
        "tos==2.3.4",
        "cachetools>=5.2.0",
        "typing-extensions>=4.4.0",
        "apscheduler>=3.10.4",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator"
    ],
    include_package_data=True,
    entry_points={
        'console_scripts': ['bw=bioos.bioos_workflow:bioos_workflow']
    })
