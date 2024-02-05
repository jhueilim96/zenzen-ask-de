from setuptools import find_packages, setup

setup(
    name="ani_dagster",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-core>=1.4.0",
        "dbt-duckdb",
        "dbt-duckdb",
        "dbt-duckdb",
        "dbt-duckdb",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)