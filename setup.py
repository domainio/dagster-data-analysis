from setuptools import setup, find_packages

setup(
    name="nyc_taxi_analysis",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "pandas",
        "pyarrow",
        "pydantic",
        "sqlalchemy",
        "plotly",
        "fastapi",
        "uvicorn",
        "click",
        "jupyter",
        "nbconvert",
        "pytest",
        "pyyaml",
        "requests",
    ],
    entry_points={
        "console_scripts": [
            "nyc-taxi-cli=src.cli.commands:cli",
        ],
    },
)
