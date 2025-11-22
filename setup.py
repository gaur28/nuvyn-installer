#!/usr/bin/env python3
"""
Setup script for Nuvyn Executor Script
A job-based metadata extraction and processing system for Databricks environments
"""

from setuptools import setup, find_packages
import os
from pathlib import Path

# Get the directory containing this file
HERE = Path(__file__).parent

# Read README file
def read_readme():
    readme_path = HERE / "README.md"
    if readme_path.exists():
        with open(readme_path, "r", encoding="utf-8") as fh:
            return fh.read()
    return "Nuvyn Executor Script - Job-based metadata extraction for Databricks"

# Read requirements
def read_requirements():
    requirements_path = HERE / "requirements.txt"
    if requirements_path.exists():
        with open(requirements_path, "r", encoding="utf-8") as fh:
            return [line.strip() for line in fh if line.strip() and not line.startswith("#")]
    # Fallback requirements if requirements.txt doesn't exist
    return [
        "pandas>=1.5.0",
        "numpy>=1.21.0",
        "aiohttp>=3.8.0",
        "requests>=2.28.0",
        "pydantic>=1.10.0",
        "python-dotenv>=0.19.0",
        "structlog>=22.1.0",
        "rich>=12.0.0",
        "databricks-sql-connector>=2.0.0",
        "databricks-cli>=0.17.0",
        "sqlalchemy>=1.4.0",
        "azure-storage-blob>=12.14.0",
        "boto3>=1.26.0",
        "azure-identity>=1.12.0",
    ]

setup(
    name="nuvyn-executor-script",
    version="1.0.0",
    author="Nuvyn.bldr Development Team",
    author_email="gaurtushar5567@gmail.com",
    description="Job-based metadata extraction and processing system for Databricks environments",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/nuvyn-bldr/executor-script",
    packages=find_packages(),  # Automatically finds both 'executor' and 'nuvyn_executor' packages
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=0.991",
        ],
        "ml": [
            "scikit-learn>=1.1.0",
            "scipy>=1.9.0",
        ],
        "notebook": [
            "jupyter>=1.0.0",
            "ipywidgets>=8.0.0",
        ],
        "monitoring": [
            "psutil>=5.9.0",
            "memory-profiler>=0.60.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "nuvyn-executor=executor.main:cli_main",
        ],
    },
    include_package_data=True,
    package_data={
        "executor": [
            "*.py",
            "schema/*.py",
            "datasource/*.py", 
            "data_reader/*.py",
            "metadata/*.py",
            "storage/*.py",
            "transport/*.py",
        ],
    },
    keywords=[
        "databricks",
        "metadata",
        "data-extraction", 
        "data-processing",
        "job-scheduler",
        "data-warehouse",
        "etl",
        "data-pipeline",
        "cloud-storage",
        "azure",
        "aws",
    ],
    project_urls={
        "Bug Reports": "https://github.com/nuvyn-bldr/executor-script/issues",
        "Source": "https://github.com/nuvyn-bldr/executor-script",
        "Documentation": "https://docs.nuvyn.bldr/executor-script",
    },
    zip_safe=False,
)
