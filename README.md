### Connecting to EC2
Generate a key pair and download the certficiate onto your local machine. 
Using the path to where you downloaded the certificate, connect to the server:
```
ssh -i "{path/to/your/kaypair.cer}" ec2-44-208-167-138.compute-1.amazonaws.com
```

## Configure AWS
You will need to have an `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` generated, see this [getting started documentation](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-prereqs.html)
Complete the environmental variables needed in `.env.sample` and move to `.env`
run the following command:
```
aws configure
```
and interactualy populate the values as required:

### Configuring AWS CLI
run the command 

```
aws configure
```

then you will be asked to interactively enter the following details:

```
AWS Access Key ID [None]: {AWS_ACCESS_KEY}
AWS Secret Access Key [None]: {AWS_SECRET_ACCESS_KEY}
Default region name [None]: {region} # e.g. us-east-1
Default output format [None]: {format} # e.g. json
```
### Enter `psql`
```
sudo su - postgres 
psql
```


### Create Postgres User and database;

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
 
## Setting up with Docker

### Adding aws ssh keypair to github repos 

On your local machine, generate a ssh keypair using the following command:

```
ssh-keygen -t rsa -b 4096 -C [your email address]
```

When interactively saving, append your first name to the key so as not to overwrite permissions.

Then alter permissions

```
$ chmod 600 ~/.ssh/id_rsa
```

and add to `known_keys`

```
ssh-add -k ~/.ssh/id_rsa
```

Then copy

```
$ cat ~/.ssh/id_rsa.pub   # copy to clipboard
```

Add this key to your settings on [github](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account) 


### Add user to docker group

```
sudo usermod -aG docker $USER
```

### Build docker image

```
docker build openaq-engine -t openaq_engine_app --build-arg ssh_prv_key="$(cat ~/.ssh/id_rsa)" --build-arg ssh_pub_key="$(cat ~/.ssh/id_rsa.pub)"
```

### Executing docker 
To enter the docker development environment run the following 

```
docker exec -it {name_of_your_docker_container} bash
```
(to find the name of the docker container run `docker ps` and it will be the one you just made!)


After entering the container, you may need to pull from the remote git repository. in order to do so with this user, please configure your github profile using the following commands:

```
git config --global user.name "{your github username}"
git config --global user.email "{yout github email address}"
```

then install the package locally using the command:

```
pip install -e .
```

To test the installation run:

```
openaq-engine --help
```