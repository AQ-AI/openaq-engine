
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
```
psql -U postgres
```
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
echo -e 'export PYENV_ROOT="$HOME/.pyenv" '
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"' >> ~/.bash_profile
```
### Reload your profile.
```
source ~/.bash_profile
```
# Install dependenies

#### Install poetry via curl
```
curl -sSL https://install.python-poetry.org | python3 -
```
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
## `pre-commit` hooks
We use `pre-commit` to check the formatting of our commits.
```
pre-commit install
```
Test the pre-commit works:
```
pre-commit run --all-files
```

# Earth engine signup
Please signup for Google Earth engine to rtreve satellite imagery, visit https://signup.earthengine.google.com/.

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
