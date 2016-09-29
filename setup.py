try:
    import setuptools
    from setuptools import setup
except ImportError:
    setuptools = None
    from distutils.core import setup

version = '0.0.1'

setup(
    name='popgo_weibo_publisher',
    version=version,
    py_modules=['popgo_weibo_publisher'],
    author='Siglud')
