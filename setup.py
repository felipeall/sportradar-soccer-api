from setuptools import setup

setup(
    name='sportradar_soccer_api',
    version='0.0.1',
    packages=["sportradar_soccer_api"],
    description='',
    author='Felipe Allegretti',
    author_email='felipe@allegretti.me',
    install_requires=[
        "pandas==1.4.4",
        "requests==2.28.0",
    ],
)
