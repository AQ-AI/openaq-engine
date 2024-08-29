
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
$ openaq-engine run-pipeline /path/to/models/dir /path/to/plots/dir --cohort-table
```

### Create Postgres User and database;
```
psql -U postgres
```
#### This only needs to be done once, skip ahead to Login

```
CREATE ROLE openaq WITH LOGIN PASSWORD 'openaq';
CREATE DATABASE openaq_db;
GRANT ALL PRIVILEGES ON DATABASE openaq_db TO openaq;
ALTER ROLE openaq SUPERUSER;
SELECT pg_reload_conf();
```
### Restart postgres
```
sudo systemctl restart postgresql-12.service
```
### Login
```
psql -U openaq -d openaq_db -h localhost -W
```
### Setting up pyenv
```
curl https://pyenv.run | bash
```
Export `pyenv` variables
Add pyenv initializer to shell startup script.

```
echo -e 'export PYENV_ROOT="$HOME/.pyenv" '
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"' >> ~/.bash_profile
```
### Reload your profile.
```
source ~/.bash_profile
```
# Install dependenies

#### Install poetry via curl
```
curl -sSL https://install.python-poetry.org | python3 -
```
### Add poetry to your shell
```
export PATH="$HOME/.poetry/bin:$PATH"
```
### For tab completion in your shell, see the documentation
```
poetry help completions
```
#### Configure poetry to create virtual environments inside the project's root directory
```
poetry config virtualenvs.in-project true
```
#### Install packages via poetry
```
poetry install
```
## `pre-commit` hooks
We use `pre-commit` to check the formatting of our commits.
```
pre-commit install
```
Test the pre-commit works:
```
pre-commit run --all-files
```

# Earth engine signup
Please signup for Google Earth engine to rtreve satellite imagery, visit https://signup.earthengine.google.com/.

If at any point the documentation does not suffice, you can always get help by emailing us at [info@aqai.xyz](mailto:info@aqai.xyz).
