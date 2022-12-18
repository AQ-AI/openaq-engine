Connecting to EC2
==============================

Generate a key pair and download the certficiate onto your local machine.
Using the path to where you downloaded the certificate, connect to the server:

.. code-block:: bash

    ssh -i "{path/to/your/keypair.cer}" ec2-44-208-167-138.compute-1.amazonaws.com

Configure AWS
--------------

You will need to have an `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` generated, see this [getting started documentation](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-prereqs.html)
Complete the environmental variables needed in `.env.sample` and move to `.env`
run the following command:

Configuring AWS CLI
~~~~~~~~~~~~~~~~~~~~


run the command

.. code-block:: bash

    aws configure

and interactualy populate the values as required. Then you will be asked to interactively enter the following details:

.. code-block:: bash

    AWS Access Key ID [None]: {AWS_ACCESS_KEY}
    AWS Secret Access Key [None]: {AWS_SECRET_ACCESS_KEY}
    Default region name [None]: {region} # e.g. us-east-1
    Default output format [None]: {format} # e.g. json

Adding new user
----------------------------


Outside the instance run the following:

.. code-block:: bash

    ssh-keygen -y -f /path_to_key_pair/key-pair-name.cer

Connect to the instance. When inside, run the following:

.. code-block:: bash

    sudo adduser newuser

Switch to the new account so that the directory and file have the proper ownership:

.. code-block:: bash

    sudo su - newuser


The prompt changes from `ec2-user` to newuser to indicate that you have switched the shell session to the new account.
Create a `.ssh` directory in the newuser home directory and change its file permissions to `700` (only the owner can read, write, or open the directory).

.. code-block:: bash

    mkdir .ssh
    chmod 700 .ssh

Create a file named authorized_keys in the `.ssh` directory and change its file permissions to 600 (only the owner can read or write to the file).

.. code-block:: bash

    touch .ssh/authorized_keys
    chmod 600 .ssh/authorized_keys


Open the authorized_keys file using your favorite text editor (such as vim or nano).

.. code-block:: bash

    vi .ssh/authorized_keys

Paste the public key that you retrieved in Step 2 into the file and save the changes.
