from setuptools import setup, find_packages
from io import open
from os import path
import pathlib

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

with open(path.join(HERE, 'requirements.txt'), encoding='utf-8') as f:
    package_requirements = f.read().split('\n')

install_requirements = [x.strip() for x in package_requirements if ('git+' not in x) and (not x.startswith('#')) and
                        (not x.startswith('-'))]

dependency_links = [x.strip().replace('git+', '') for x in package_requirements if 'git+' not in x]

setup(
    name='edl',
    description='Presenting data lineage based on your data warehouse query history',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requirements,
    python_requires='>=3.6.2',
    entry_points='''
        [console_scripts]
        edl=lineage.main:main
    ''',
    author="Elementary",
    keyword="data, lineage, data lineage, data warehouse, DWH",
    long_description=README,
    long_description_content_type="text/markdown",
    license='',
    url='https://github.com/TODO',
    download_url='https://github.com/TODO',
    dependency_links=dependency_links,
    author_email='or@elementary-data.com',
    classifiers=[
        #"License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
)