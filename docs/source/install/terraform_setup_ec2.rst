Terraform Setup for AWS EC2 Instance
====================================

Setup your AWS Environments before using Terraform
--------------------------------------------------

Before you begin, make sure your AWS environment is set up correctly by following the prerequisites described in the :ref:`aws-prerequisites` section.



Terraform Code to Initialize AWS EC2 Instance
---------------------------------------------

Create a file named `main.tf` in the `terraform-config-file` directory with the following content:

.. code-block:: hcl

   provider "aws" {
     region = "us-east-1" # Replace with your desired AWS region
   }

   resource "aws_key_pair" "aqai-web-server-access" {
     key_name   = "example-key"
     public_key = file("~/.ssh/id_rsa.pub") # Replace with the path to your public SSH key
   }

   resource "aws_instance" "aqai-web-server" {
     ami           = "ami-0cff7528ff583bf9a" # Replace with your desired Amazon Machine Image (AMI) ID
     instance_type = "t2.micro"
     key_name      = aws_key_pair.example.key_name

     tags = {
       Name = "aqai-web-server-ec2-instance"
     }
   }

Initialize Terraform
--------------------

To initialize Terraform, run the following command in the directory containing your `main.tf` file:

.. code-block:: bash

   terraform init

Apply the Terraform Configuration
---------------------------------

To apply the Terraform configuration and create your EC2 instance, run:

.. code-block:: bash

   terraform apply

Destroy the EC2 Instance
------------------------

To avoid being charged, you can destroy the EC2 instance with the following command:

.. code-block:: bash

   terraform destroy

Find the Latest Amazon Machine Image (AMI)
------------------------------------------

To find the latest Amazon Machine Image (AMI) ID, use the AWS CLI:

.. code-block:: bash

   aws ec2 describe-images --owners amazon --query 'sort_by(Images, &Name)[-1].ImageId' --output text --region us-east-1 --filters 'Name=name,Values=amzn3-ami-hvm-2.0.????????.?-x86_64-gp2' 'Name=state,Values=available'

This command will provide the latest AMI ID available in the specified region.
