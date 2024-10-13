from setuptools import setup, find_packages

# Read requirements from the requirements.txt file
with open("requirements.txt") as f:
    install_requires = [line.strip() for line in f if line.strip()]

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
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
    install_requires=install_requires
)
