from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="oic-devops",
    version="0.1.0",
    author="Sean Holt",
    author_email="spam@integritycrank.com",
    description="A package for managing Oracle Integration Cloud DevOps",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/WolVesz/oic-devops",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "requests>=2.25.0",
        "pyyaml>=5.4.1",
        "click>=8.0.0",
        "python-dateutil>=2.8.1",
        "jsonschema>=3.2.0",
    ],
    entry_points={
        "console_scripts": [
            "oic-devops=oic_devops.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "oic_devops": ["config-template.yaml"],
    },
)
