"""
Setup configuration for Better-DBT-Metrics
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="better-dbt-metrics",
    version="2.0.0",
    author="Better-DBT-Metrics Team",
    author_email="team@better-dbt-metrics.com",
    description="GitHub Actions-first approach to dbt semantic layer with powerful DRY features",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rdwburns/better-dbt-metrics",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "click>=8.0.0",
        "pyyaml>=6.0",
        "jinja2>=3.0.0",
        "jsonschema>=4.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.950",
            "pytest-cov>=3.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "better-dbt-metrics=cli.main:cli",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["templates/**/*.yml", "schemas/*.json"],
    },
)