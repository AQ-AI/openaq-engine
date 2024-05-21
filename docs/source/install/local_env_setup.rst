
Local Environment Setup
=========================

Setting up pyenv
---------------------

Curl the following link to download pyenv

.. code-block:: bash

    curl https://pyenv.run | bash

Export `pyenv` variables:
Add pyenv initializer to shell startup script.

.. code-block:: 

    echo -e 'export PYENV_ROOT="$HOME/.pyenv" '
    export PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init --path)"
    eval "$(pyenv init -)"' >> ~/.bash_profile

Reload your profile.

.. code-block:: bash

    source ~/.bash_profile

Install dependenies
---------------------

Install poetry via curl:

.. code-block:: bash

    curl -sSL https://install.python-poetry.org | python3 -

Add `poetry` to your shell

.. code-block:: bash

    export PATH="$HOME/.poetry/bin:$PATH"

Configure poetry to create virtual environments inside the project's root directory

.. code-block:: bash

    poetry config virtualenvs.in-project true

Install packages via poetry

.. code-block:: bash

    poetry install

We use `pre-commit` to check the formatting of our commits.

.. code-block:: bash

    pre-commit install


Test the pre-commit works:

.. code-block:: bash

    pre-commit run --all-files

