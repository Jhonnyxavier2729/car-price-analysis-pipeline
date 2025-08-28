from setuptools import find_packages, setup

setup(
    name="pipeline_car",
    packages=find_packages(exclude=["pipeline_car_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
