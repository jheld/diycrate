import os
from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...


def read(file_name):
    return open(os.path.join(os.path.dirname(__file__), file_name)).read()


packages = ['bottle==0.12.9',
            'boxsdk==1.5.1',
            'decorator==4.0.9',
            'enum34==1.1.2',
            'ipython==4.1.2',
            'ipython-genutils==0.1.0',
            'path.py==8.1.2',
            'pexpect==4.0.1',
            'pickleshare==0.6',
            'ptyprocess==0.5.1',
            'pyinotify==0.9.6',
            'redis==2.10.5',
            'requests==2.9.1',
            'requests-toolbelt==0.6.0',
            'simplegeneric==0.8.1',
            'six==1.10.0',
            'traitlets==4.2.1',
            'wheel==0.29.0',
            'pyopenssl==16.0.0',
            'cherrypy==5.1.0']

setup(
    name="diycrate",
    version="0.2.12",
    author="Jason Held",
    author_email="jasonsheld@gmail.com",
    description=("box.com for linux -- unofficial, based on python SDK"),
    license="MIT",
    keywords="cloud storage box.com sdk linux",
    url="http://packages.python.org/diycrate",
    install_requires=packages,
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
        'Intended Audience :: Developers',
        'Operating System :: POSIX :: Linux',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: End Users/Desktop',
        'Programming Language :: Python :: 3.5',
    ],
)