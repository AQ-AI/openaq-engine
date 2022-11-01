from setuptools import find_packages, setup

setup(
    name="openaq-engine",
    packages=find_packages(),
    version="0.1.0",
    install_requires=[
        "Click",
    ],
    entry_points="""
        [console_scripts]
        openaq-engine=main:cli
    """,
    description="Library to query openaq data",
    author="Christina Last",
    license="",
)
