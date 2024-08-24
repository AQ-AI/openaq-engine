from setuptools import find_packages, setup

setup(
    name="openaq-engine",
    version="0.1.0",
    packages=find_packages(
        include=["openaq_engine", "openaq_engine.*"]
    ),  # Explicitly include your package
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
    package_dir={
        "openaq_engine": "."
    },  # Map the 'openaq_engine' package to the current directory
)
