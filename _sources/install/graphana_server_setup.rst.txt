How To Install Grafana Server
=======================

Step 1: Create an IAM Role and Policy
-------------------------------------

- Go to IAM
- Create Policy
- Insert the policy below into the JSON

.. code-block:: json

   {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Sid": "AllowReadingMetricsFromCloudWatch",
              "Effect": "Allow",
              "Action": [
                  "cloudwatch:ListMetrics",
                  "cloudwatch:GetMetricStatistics",
                  "cloudwatch:GetMetricData"
              ],
              "Resource": "*"
          },
          {
              "Sid": "AllowReadingTagsInstancesRegionsFromEC2",
              "Effect": "Allow",
              "Action": [
                  "ec2:DescribeTags",
                  "ec2:DescribeInstances",
                  "ec2:DescribeRegions"
              ],
              "Resource": "*"
          }
      ]
   }

- Create Role
- EC2
- Choose Grafana policy
- Name it and Create a Role

Step 2: Create a Security Group
-------------------------------

- Our Security Group will need ports 22, 80, and 3000 open, Port 3000 is for Grafana
- Go to Security Groups
- Add Port 22, 80, and 3000 for our inbound rules pointing to all CIDR ranges.

Step 3: Create EC2 Instance
---------------------------

- Go to EC2
- Create EC2
- Choose a public subnet
- Give it our Grafana security group
- Now SSH into our instance

Update your packages with

.. code-block:: bash

   sudo yum update -y

Install Git
~~~~~~~~~~~

- To start you will need to update your server and then install git.

.. code-block:: bash

   sudo yum install git -y

- Clone this repo from grafana, and change the directory into tutorial-environment.
- We need to add a repository for grafana so our OS will know where it is.

.. code-block:: bash

   sudo nano /etc/yum.repos.d/grafana.repo

Add the text below to the repo file. This will install the open-source Grafana.

.. code-block:: none

   [grafana]
   name=grafana
   baseurl=https://packages.grafana.com/oss/rpm
   repo_gpgcheck=1
   enabled=1
   gpgcheck=1
   gpgkey=https://packages.grafana.com/gpg.key
   sslverify=1
   sslcacert=/etc/pki/tls/certs/ca-bundle.crt

Step 4: Install Grafana
-----------------------

- Now we will install Grafana. If you wish to actually test Grafana skip to Section Two below before you install.

.. code-block:: bash

   sudo yum install grafana -y

- The next command will reload the system

.. code-block:: bash

   sudo systemctl daemon-reload

- Then we will start our server and check our service with the following two commands.

.. code-block:: bash

   sudo systemctl start grafana-server
   sudo systemctl status grafana-server

- Our final command will ensure that Grafana will start up automatically if we stop and restart our instance.

.. code-block:: bash

   sudo systemctl enable grafana-server.service

Test Grafana
------------

- To test our server we will need to grab the public IPv4 from AWS and add a :3000 at the end, ie. ec2-43-XXX-YYYY:3000, and insert it into our browser URL.
- This will bring you to a login screen and our Username and Password will be admin and admin.
- You will be prompted to create a new password.

Using Grafana
-------------

- Then we can access our application which is a Grafana News app, this app will allow us to make some API calls and generate some errors so we can see Grafana in action.
- Go to your AWS console, grab your public IP and enter it in the URL with 8081, like so 10.10.10.10:8081. If you get here and cannot access your website make sure you open it up on port 3000!

- Once in the Grafana news app enter a name and in the URL enter example.com. Then vote for the title in the top right.
- Now we need to actually get to the Grafana website, so we will need to grab our public IPv4 from the AWS console and add a :3000 at the end, ie. 10.90.80.10:3000, and insert it into our browser URL. This will bring you to a login screen and our Username and Password will be admin. You will be prompted to create a new password.
