# Welcome to openaq-engine's Documentation!

![CI](https://img.shields.io/github/actions/workflow/status/AQ-AI/openaq-engine/.github/workflows/workflow.yaml?branch=develop)
![Coverage](https://img.shields.io/badge/coverage-80%25-brightgreen)

The documentation is built up in the following parts:

First, there is the quickstart tutorial which aims at getting you started with openaq-engine as quickly as possible. This is the right place for you if you just want to get a feel for the library or if you have never used openaq-engine before.

In case this does not suffice, we also have an API reference: [Module Index](#modindex).

If you want to develop for openaq-engine and contribute, check out our guidelines: [Contribution Guidelines](#contribution).

Our license is available here: [License](#license).

Our code of conduct is available here: [Code of Conduct](#code_of_conduct).

Our community statement is available here: [Community Statement](#community-statement).

## Quick answer:
```bash
$ git clone git@github.com:AQ-AI/openaq-engine.git
$ cd openaq-engine
$ poetry install
$ poetry shell
$ cd openaq_engine
$ pip install -e .
$ cd ..
$ openaq-engine --help
```

To run the pipeline globally run:
```bash
$ openaq-engine run-pipeline /path/to/models/dir /path/to/plots/dir pollutant pm25 --source openaq-api
```

Remember to define your psql environment variables and export them using:
```bash
$ source .env
```

## Long answer:
We keep more detailed installation instructions (including dependencies) up-to-date below.

If at any point the documentation does not suffice, you can always get help by emailing us at [info@aqai.xyz](mailto:info@aqai.xyz).
