#!/usr/bin/env python

from os.path import join, dirname
from setuptools import setup

filename=join(dirname(__file__), 'KafkaLibrary', 'version.py')
exec(compile(open(filename).read(),filename, 'exec'))

DESCRIPTION = """
Kafka support for Robot Framework.
"""[1:-1]

setup(name         = 'robotframework-kafkalibrary',
      version      = VERSION,
      description  = 'Kafka library for Robot Framework',
      long_description = DESCRIPTION,
      author       = 'Marcin Mierzejewski',
      author_email = '<mmierz@gmail.com>',
      url          = 'https://github.com/s4int/robotframework-KafkaLibrary',
      license      = 'Apache License 2.0',
      keywords     = 'robotframework testing kafka',
      platforms    = 'any',
      classifiers  = [
          "Development Status :: 4 - Beta",
          "License :: OSI Approved :: Apache Software License",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Topic :: Software Development :: Testing"
      ],
      install_requires = [
          'robotframework >= 2.6.0',
          'kafka-python',
      ],
      packages    = ['KafkaLibrary'],
      )
