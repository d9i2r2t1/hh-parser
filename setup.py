from typing import List

from pkg_resources import parse_requirements
from setuptools import find_packages, setup

module_name = 'hh_parser'


def load_requirements(file_name: str) -> List[str]:
    """
    Прочитай requirements из файла.
    :param file_name: название файла с requirements
    """
    requirements = []
    with open(file_name, 'r') as file:
        for r in parse_requirements(file.read()):
            requirements.append(f'{r.name}{r.specifier}')
    return requirements


setup(
    name=module_name,
    version='0.2.0',
    author='Oleg Denisov',
    author_email='dirt-rider@yandex.ru',
    license='MIT',
    description='Package for parsing hh.ru',
    long_description=open('README.rst').read(),
    url='https://github.com/oleg-dirtrider/hh_parser.git',
    platforms='Linux',
    python_requires='>=3.8',
    packages=find_packages(),
    install_requires=load_requirements('requirements.txt'),
    entry_points={
        'console_scripts': [
            f'{module_name}-run = {module_name}.__main__:main',
        ]
    },
    include_package_data=True
)
