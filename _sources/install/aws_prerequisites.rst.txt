.. _aws-prerequisites:

AWS Prerequisites 
=================

To create an EC2 instance on AWS with Terraform, you’ll need to have the following prerequisites in place:

AWS Account
-----------

You must have an AWS account to create and manage resources on the AWS cloud. If you don’t already have this, you can `sign up for an account and use the free tier here <https://aws.amazon.com/free/>`_. For the first 12 months, you can run a free EC2 instance with the following specifications:

- 750 hours per month of Linux, RHEL, or SLES t2.micro or t3.micro instance (dependent on region)
- 750 hours per month of Windows t2.micro or t3.micro instance (dependent on region)

**Note:** If you continue to run your EC2 instance after the 12-month free tier allowance period is up, you’ll start getting charged, so remember to clean up after the tutorial!

Terraform Installed
-------------------

You’ll need to have Terraform installed on your machine to write and execute Terraform code. You can `download the appropriate version for your machine here <https://www.terraform.io/downloads.html>`_. Extract the downloaded zip file to a directory on your machine and add the directory containing the Terraform binary to your system’s PATH environment variable.

AWS CLI Installed & IAM User with Permissions
---------------------------------------------

You’ll need to have the AWS Command Line Interface (CLI) installed on your machine to interact with EC2 instances and other AWS resources from the command line. You can `download the appropriate version for your system here <https://aws.amazon.com/cli/>`_. Once you have installed the AWS CLI, you can configure it by running the following command in a terminal window:

.. code-block:: bash

   aws configure

This will prompt you to enter your AWS access key ID, secret access key, default region, and default output format. You can obtain your access key ID and secret access key from the AWS Management Console by navigating to the security credentials section once logged in or create a new one from there if needed.

If you have not already done so, you should create an IAM user with the minimum required permissions necessary. Learn more about `AWS IAM best practices <https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html>`_.

SSH Key Pair
------------

To access a Linux-based EC2 instance via SSH, you’ll need an SSH key pair. Run the following command to generate a new SSH key pair:

.. code-block:: bash

   ssh-keygen -t rsa -b 4096

This will create a new RSA key pair with a length of 4096 bits. You will be prompted to enter a file name to save the key pair. The default location is in your user’s home directory under the `.ssh` directory. You can choose a different file name or directory if you prefer. You will be prompted to enter a passphrase for the key pair. This is optional but recommended to add an extra layer of security.

The `ssh-keygen` command will generate two files: a private key file and a public key file. The private key file should be kept secure and never shared with anyone. The public key file can be shared with Amazon EC2 instances to allow SSH access.

Finally, to use the key pair with an Amazon EC2 instance, you must add the public key to the instance when you configure it with Terraform.

Authentication with AWS
-----------------------

You can configure Terraform to authenticate with AWS using several methods. With AWS CLI installed, you can use the named profiles method, which is a recommended approach for authenticating Terraform to AWS because it allows you to manage multiple sets of AWS credentials and control access to resources using IAM roles and policies.

First, ensure the AWS CLI is installed and configured using the guidelines in the prerequisite section to add the AWS access key ID and secret access key using the `aws configure` command. By default, the AWS CLI-named profiles use the same access key ID and secret access key as your default profile, so you don’t need to specify them again for different profiles.

Create an AWS CLI named profile for Terraform in the `~/.aws/config` file (Linux and macOS) or `%UserProfile%\.aws\config` file (Windows). You can override the default access key ID and secret access key for a named profile by setting the `aws_access_key_id` and `aws_secret_access_key` attributes if required.

Although it is not necessary (and may cause problems if one of your team uses the code in a different SDK), you can add a new profile section with a unique profile name, such as `jack.roper`. The example below tells the AWS CLI to use the `us-west-2` region for the `jack.roper` profile.

.. code-block:: ini

   [profile jack.roper]
   region = us-west-2

In your Terraform configuration file, the provider block can be configured as follows to reference the named profile:

.. code-block:: hcl

   provider "aws" {
     region  = "us-west-2"
     profile = "jack.roper"
   }

Note you can also authenticate using environment variables:

.. code-block:: bash

   export AWS_ACCESS_KEY_ID=your_access_key_id
   export AWS_SECRET_ACCESS_KEY=your_secret_access_key
