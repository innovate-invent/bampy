from setuptools import setup, find_packages
from bampy.__version import __version__

with open('README.md') as readme:
    setup(
        name='bampy',
        version=__version__,
        packages=find_packages(),
        long_description=readme.read(),
        url='https://github.com/innovate-invent/bampy',
        license='MIT',
        author='Nolan',
        author_email='nolan@i2labs.ca',
        description='Python implementation of htslib supporting BAM, SAM, and BGZF compression',
        include_package_data=True
    )
