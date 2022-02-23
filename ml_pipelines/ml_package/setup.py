from setuptools import setup, find_packages

setup(
    name='ml_package',
    version='0.1.0',
    packages=find_packages(include=['ml_package', 'ml_package.*'])
)