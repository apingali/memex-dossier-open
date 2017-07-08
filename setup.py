'''Memex Dossier Stack prototypes

.. This software is released under an MIT/X11 open source license.
   Unpublished Work Copyright 2012-2016 Diffeo, Inc.
'''
import os

from setuptools import setup, find_packages
import distutils.cmd
import distutils.log


from version import get_git_version
VERSION, SOURCE_LABEL = get_git_version()
PROJECT = 'memex_dossier'
AUTHOR = 'Diffeo, Inc.'
AUTHOR_EMAIL = 'support@diffeo.com'
URL = None
DESC = 'Active learning & discovery models for Memex'


def read_file(file_name):
    file_path = os.path.join(os.path.dirname(__file__), file_name)
    return open(file_path).read()


class DataInstallCommand(distutils.cmd.Command):
    '''installs nltk data'''

    user_options = []
    description = '''installs nltk data'''

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import nltk
        from memex_dossier.models.tests.test_features import nltk_data_packages
        for data_name in nltk_data_packages:
            print('nltk.download(%r)' % data_name)
            nltk.download(data_name)

setup(
    name=PROJECT,
    version=VERSION,
    description=DESC,
    license='MIT',
    long_description=read_file('README.md'),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages=find_packages(),
    cmdclass={'install_data': DataInstallCommand},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities',
        'License :: OSI Approved :: MIT License',
    ],
    install_requires=[
        'backports.lzma!=0.0.4',
        'backport-collections',
        'beautifulsoup4',
        'bottle',
        'cbor',
        'certifi',
        'coordinate',
        'dblogger',
        'dossier.label',
        'elasticsearch < 2',
        'enum34',
        'geojson',
        'gensim',
        'happybase',
        'kvlayer',
        'many_stop_words',
        'mmh3',
        'names',
        'nilsimsa',
        'nltk < 3.1', # regexp tokenizer is no longer working with sentence_re
        'numpy',
        'pika',
        'phonenumberslite',
        'python-geohash',
        'pytest',
        'pytest-diffeo >= 0.1.4',
        'regex',
        'requests',
        'scikit-learn',
        'streamcorpus',
        'streamcorpus-pipeline',
        'trollius',
        'urlnorm >= 1.1.3',
        'wordsegment',
        'yakonfig',
    ],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'memex_dossier.models = memex_dossier.models.web.run:main',
            'memex_dossier.models.soft_selectors = memex_dossier.models.soft_selectors:main',
            'memex_dossier.models.linker = memex_dossier.models.linker.run:main',
            'memex_dossier.etl = memex_dossier.models.etl:main',
            'memex_dossier.akagraph = memex_dossier.akagraph.core:main',
            'memex_dossier.structured = memex_dossier.streamcorpus_structured.run:main',
            'memex_dossier.snsfetcher = memex_dossier.snsfetcher.run:main',
            'memex_dossier.handles.ngrams = memex_dossier.handles.ngrams:main',
            'memex_dossier.nltk_download = memex_dossier.models.tests:nltk_data',
        ],
        #'streamcorpus_pipeline.stages': [
        #    'to_memex_dossier_store = memex_dossier.models.etl.interface:to_memex_dossier_store',
        #    'structured_features = memex_dossier.streamcorpus_structured.transform:structured_features',
        #],
    },
    package_data={
#        'models': ['twitter-clusters.json'],
        'handles': ['enable1.txt', 'countries.txt'],
    },
)

