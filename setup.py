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
    name='elementary-lineage',
    description='Presenting data lineage based on your data warehouse query history',
    version='0.0.4',
    packages=find_packages(),
    include_package_data=True,
    python_requires='>=3.6.2',
    entry_points='''
        [console_scripts]
        edl=lineage.main:main
    ''',
    author="Elementary",
    keyword="data, lineage, data lineage, data warehouse, DWH",
    long_description=README,
    install_requires=install_requirements + [
        'jinja2<3.0.0',
        'werkzeug<2.0',
        'urllib3<1.26'
    ],
    long_description_content_type="text/markdown",
    license='',
    url='https://github.com/elementary-data/elementary-lineage',
    dependency_links=dependency_links,
    author_email='or@elementary-data.com',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)