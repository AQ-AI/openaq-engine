### What happened before you got here
If you are inside the ec2 instance hosting this project then your SSH key has already been added to this instance's `authorized_keys`.

## What to do next

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
docker build openaq-engine --build-arg ssh_prv_key="$(cat ~/.ssh/id_rsa)" --build-arg ssh_pub_key="$(cat ~/.ssh/id_rsa.pub)"
```

### Executing docker 
To enter the docker development environment run the following 
```
docker exec -it {name_of_your_docker_container} bash
```
(to find the name of the docker container run `docker ps` and it will be the one you just made!)

```
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