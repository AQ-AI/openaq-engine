
# Welcome to openaq-engine’s documentation!

*⁠ Documentation ⁠* | *⁠ Build ⁠*  |
------------------- | ------------------- |
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://www.aqai.xyz/openaq-engine/) | ![CI](https://img.shields.io/github/actions/workflow/status/AQ-AI/openaq-engine/.github/workflows/workflow.yaml?branch=develop) |

The documentation is built up in the following parts:

First, there is the quickstart tutorial which aims at getting you started with openaq-engine as quickly as possible. This is the right place for you if you just want to get a feel for the library or if you have never used openaq-engine before.

In case this does not suffice, we also have an API reference: [API Index](https://www.aqai.xyz/openaq-engine/genindex.html)

If you want to develop for openaq-engine and contribute, check out our guidelines: [Contribution Guidelines](https://github.com/AQ-AI/openaq-engine/blob/develop/.github/CONTRIBUTING.md)

Our license is available here: [License](https://github.com/AQ-AI/openaq-engine?tab=BSD-3-Clause-1-ov-file#readme)

Our code of conduct is available here: [Code of Conduct](https://github.com/AQ-AI/openaq-engine?tab=coc-ov-file#readme)

Our community statement is available here: [Community Statement](https://github.com/AQ-AI/openaq-engine/blob/develop/community-statement.md)

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
$ openaq-engine run-pipeline /path/to/models/dir /path/to/plots/dir --cohort-table
```

If at any point the documentation does not suffice, you can always get help by emailing us at [info@aqai.xyz](mailto:info@aqai.xyz).
