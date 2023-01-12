Docker setup
====================

Adding aws ssh keypair to github repos
------------------------------------------

On your local machine, generate a ssh keypair using the following command:

.. code-block:: bash

    ssh-keygen -t rsa -b 4096 -C [your email address]


When interactively saving, append your first name to the key so as not to overwrite permissions. 
Alter permissions such that:

.. code-block:: bash

    chmod 600 ~/.ssh/id_rsa

and add to `known_keys`

.. code-block:: rst

    ssh-add -k ~/.ssh/id_rsa


Then copy:

.. code-block:: rst

    cat ~/.ssh/id_rsa.pub   # copy to clipboard


Add this key to your settings on [github](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account)

Add user to docker group
------------------------------------------

.. code-block:: bash

    sudo usermod -aG docker $USER

Build docker image

.. code-block:: bash

    docker build openaq-engine -t openaq_engine_app --build-arg ssh_prv_key="$(cat ~/.ssh/id_rsa)" --build-arg ssh_pub_key="$(cat ~/.ssh/id_rsa.pub)"

Docker setup
====================

Executing docker
----------------------------

To enter the docker development environment run the following

.. code-block:: bash

    docker exec -it {name_of_your_docker_container} bash

(to find the name of the docker container run `docker ps` and it will be the one you just made!)


After entering the container, you may need to pull from the remote git repository. in order to do so with this user, please configure your github profile using the following commands:

.. code-block:: bash

    git config --global user.name "{your github username}"
    git config --global user.email "{yout github email address}"


then install the package locally using the command:

.. code-block:: bash

    pip install -e .

To test the installation run:

.. code-block:: bash

    openaq-engine --help
