from setuptools import find_packages, setup

setup(
    name="nyc_taxi_dag",
    packages=find_packages(exclude=["nyc_taxi_dag_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
