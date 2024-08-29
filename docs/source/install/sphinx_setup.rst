# Building and Deploying Sphinx Documentation from Scratch

This guide outlines the steps to set up and deploy Sphinx documentation using GitHub Actions. The workflow builds the documentation upon every push to the `main` or `develop` branches and deploys it to the `gh-pages` branch.

## Workflow Overview

- **Name:** The workflow is named "Sphinx build and deploy."
- **Trigger:** The workflow triggers on a push to either the `main` or `develop` branch.
- **Jobs:** The workflow defines a single job (`build`) that runs on `ubuntu-latest`.

## Steps Breakdown

### 1. Check out the Repository

- **Action:** Uses `actions/checkout@v3`
- **Purpose:** Clones the repository so the workflow can access the files.

  - name: Check out the repository  
    uses: actions/checkout@v3

### 2. Set up Python

- **Action:** Uses `actions/setup-python@v3`
- **Purpose:** Sets up Python 3.11 (as specified) on the runner.

  - name: Set up Python  
    uses: actions/setup-python@v3  
    with:  
      python-version: '3.11'

### 3. Install Poetry

- **Action:** Run a script to install Poetry, the Python dependency management tool.
- **Purpose:** Installs Poetry to manage dependencies and build the project.

  - name: Install Poetry  
    run: |  
      curl -sSL https://install.python-poetry.org | python3 -  
      poetry --version

### 4. Install Dependencies

- **Action:** Runs `poetry install`
- **Purpose:** Installs the project's dependencies specified in the `pyproject.toml` file.

  - name: Install dependencies  
    run: |  
      poetry install

### 5. Build HTML

- **Action:** Runs `poetry run sphinx-build -b html docs/source docs/build/html`
- **Purpose:** Builds the Sphinx documentation and outputs the HTML files to `docs/build/html`.

  - name: Build HTML  
    run: |  
      PYTHONPATH=$PYTHONPATH:$(pwd) poetry run sphinx-build -b html docs/source docs/build/html

### 6. Upload Artifacts

- **Action:** Uses `actions/upload-artifact@v3`
- **Purpose:** Uploads the built HTML documentation as an artifact that can be accessed from the GitHub Actions page.

  - name: Upload artifacts  
    uses: actions/upload-artifact@v3  
    with:  
      name: html-docs  
      path: docs/build/html/

### 7. Deploy

- **Action:** Uses `peaceiris/actions-gh-pages@v3`
- **Purpose:** Deploys the contents of `docs/build/html` to the `gh-pages` branch. The `github_token` is used for authentication.

  - name: Deploy  
    uses: peaceiris/actions-gh-pages@v3  
    with:  
      github_token: ${{ secrets.GITHUB_TOKEN }}  
      publish_dir: docs/build/html

## Additional Notes

- **PYTHONPATH Adjustment:** Make sure the Python path is set correctly to find your modules.
- **Mock Imports:** If there are modules that aren't needed for the documentation, consider mocking them in your `conf.py`.
- **Dependencies:** Ensure all dependencies are listed in `pyproject.toml` and properly installed.

By following these steps, you will be able to successfully build and deploy Sphinx documentation using GitHub Actions.
