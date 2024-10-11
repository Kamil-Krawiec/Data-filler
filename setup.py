from setuptools import setup, find_packages

setup(
    name='intelligent-data-generator',
    version='0.1.0',
    author='Kamil Krawiec',
    author_email='kamil.krawiec9977@gmail.com',
    description='A Python package for generating semantically and syntactically correct data for RDBMS.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Kamil-Krawiec/Data-filler.git',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: Apache License 2.0',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
    install_requires=[line.strip() for line in open("./requirements.txt").readlines()]
)
