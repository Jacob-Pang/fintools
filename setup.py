from glob import glob
from setuptools import setup, find_packages
from os.path import basename, splitext

with open("README.md", 'r') as f:
    long_description = f.read()

print(find_packages("src"))
setup(
    name="fintools",
    version="1.0",
    description="modules for finance stuff",
    long_description=long_description,
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[
        splitext(basename(path))[0] for path in glob('src/*.py')
    ],
    data_files=glob('src/**/*.json', recursive=True),
    include_package_data=True,
    install_requires=[
        "beautifulsoup4", "openpyxl", "pandas", "requests"
    ]
)