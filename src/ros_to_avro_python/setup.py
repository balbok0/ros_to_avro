from distutils.core import setup
from xml.etree import ElementTree
from pathlib import Path


def get_version():
    tree = ElementTree.parse(Path(__file__).parent / 'package.xml')
    root = tree.getroot()
    version = None
    for child in root:
        if child.tag == "version":
            version = child.text
            break
    return version


setup(
    version=get_version(),
    packages=['avro_sink'],
    package_dir={'': 'src'},
    install_requires=[
        "avro==1.11.1",
    ]
)
