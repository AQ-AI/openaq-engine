Welcome to openaq-engine's documentation!
=========================================================


The documentation is build up in the following parts: first, there is the
quickstart tutorial which aims at getting you started with openaq-engine as quickly as
possible. This is the right place for you if you just want get a feel for the
library or if you never used openaq-engine before.


In case this does not suffice, we also have an API reference, the
:ref:`modindex`. 

If you want to develop for openaq-engine and contribute, check out our guidelines in: :ref:`contribution`

Our license is in: :ref:`license`

Our code of conduct is in: :ref:`code_of_conduct`

Our community statement is in: :ref:`community-statement`

Quick answer:
::

	$ git clone git@github.com:AQ-AI/openaq-engine.git
	$ poetry install

Remember to define your psql environment variables and export them using:
::
	
   $ source .env 

Long answer:
We keep more detailed installation instructions (including dependencies)
up-to-date below

If at any point the documentation does not suffice, you can always get help by mailing at info@aqai.xyz

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   open_source/license
   open_source/contribution
   open_source/code_of_conduct
   open_source/community_statement
   install/aws_prerequisites
   install/aws_setup
   install/terraform_setup_ec2
   install/postgres_setup
   install/docker_setup
   install/environment_variables  
   install/local_env_setup
   install/graphana_server_setup
   install/architecture
   install/database_schema
   install/mlflow_setup
   install/sphinx_setup

.. include:: genindex.rst


------------


we also have an API reference, the :ref:`modindex`. 


.. autosummary::
   :toctree: _autosummary
   :template: custom-module-template.rst
   :recursive:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`