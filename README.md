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

### Adding new user
Outside the instance run the following:
```
ssh-keygen -y -f /path_to_key_pair/key-pair-name.cer
```
Connect to the instance. When inside, run the following:
```
sudo adduser newuser
```
Switch to the new account so that the directory and file have the proper ownership:
```
sudo su - newuser
```
The prompt changes from `ec2-user` to newuser to indicate that you have switched the shell session to the new account.
Create a `.ssh` directory in the newuser home directory and change its file permissions to `700` (only the owner can read, write, or open the directory).
```
mkdir .ssh
chmod 700 .ssh
```
Create a file named authorized_keys in the `.ssh` directory and change its file permissions to 600 (only the owner can read or write to the file).

```
touch .ssh/authorized_keys
chmod 600 .ssh/authorized_keys
```

Open the authorized_keys file using your favorite text editor (such as vim or nano).
```
vi .ssh/authorized_keys
```
Paste the public key that you retrieved in Step 2 into the file and save the changes.

### Connecting Github to the Ec2 Instance over ssh
Run the following commands to generate your SSH keys on the EC2 instance
```
ssh-keygen -t rsa -C "your-email@gmail.com"
```
Switch to the root user and access your SSH public key by using the command below:
```
cat ~/.ssh/id_rsa.pub
```
Add the Public SSH key to your Github Account [using these instructions](https://help.github.com/en/github/authenticating-to-github/adding-a-new-ssh-key-to-your-github-account)

Clone the repo in your chosen directory on the instance
```
git clone git@github.com:AQ-AI/openaq-engine.git
```

### Create Postgres User and database;
#### This only needs to be done once, skip ahead to Login

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
### Setting up pyenv
```
curl https://pyenv.run | bash
```
Export `pyenv` variables
Add pyenv initializer to shell startup script.

```
echo -e 'export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"' >> ~/.bash_profile
```
### Reload your profile.
```
source ~/.bash_profile
```
#### Install poetry via curl
curl -sSL https://install.python-poetry.org | python3 -

### Add poetry to your shell
```
export PATH="$HOME/.poetry/bin:$PATH"
```
### For tab completion in your shell, see the documentation
```
poetry help completions
```
#### Configure poetry to create virtual environments inside the project's root directory
```
poetry config virtualenvs.in-project true
```
#### Install packages via poetry
```
poetry install
```
## Exporting environment variables
```
export $(grep -v '^#' .env | xargs -d '\n')
```
## 
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
