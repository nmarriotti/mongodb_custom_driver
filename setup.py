from setuptools import setup

setup(
    name='healthandstatus',
    version='2.1.1',    
    description='Custom MongoDB driver to ingest H&S .dat files',
    url='',
    author='Nicholas Marriotti',
    author_email='nmmarriotti@scires.com',    
    license='MIT',
    packages=['healthandstatus'],
    install_requires=['pymongo>=3.11.3'],
    classifiers=[
    "Programming Language :: Python :: 2",    
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    ],
    python_requires='>=2.7'
)    
