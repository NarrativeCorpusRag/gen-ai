from setuptools import find_packages, setup

setup(
    name="gen_ai_pipeline",
    packages=find_packages(exclude=["gen_ai_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
