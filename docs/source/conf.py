import os
import sys


# Add the openaq_engine directory to the Python path
# Adjust the sys.path to point to the correct directory
sys.path.insert(0, os.path.abspath('../../openaq_engine'))

# # Add the config and mlflows directories to the Python path if needed
# sys.path.insert(0, os.path.abspath('../openaq_engine/config'))
# sys.path.insert(0, os.path.abspath('../openaq_engine/mlflows'))

print("Current working directory: " + os.getcwd())
print("Python path includes: " + str(sys.path))

# -- Project information -----------------------------------------------------
project = "openaq-engine"
copyright = "2024, Christina Last, Prithviraj Pramanik"
author = "Christina Last, Prithviraj Pramanik"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
]

autosummary_generate = True
templates_path = ["_templates"]
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
html_theme = "alabaster"
