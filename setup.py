from setuptools import setup, find_packages

setup(
    name="py-transmuter",
    version="0.1.0",
    author="Rodrigo De Rosa",
    author_email="rodrigoderosa15@gmail.com",
    description="A library that facilitates mapping and aggregating objects from one model to another",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="http://github.com/RodrigoDeRosa/py-transmuter",
    packages=find_packages(exclude=("test", "docs")),
    install_requires=[
        "pydantic==2.5.3",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.12",
        "Framework :: Pydantic :: 2",
    ],
    python_requires=">=3.12",
)
