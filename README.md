### Connecting to EC2
Generate a key pair and download the certficiate onto your local machine. 
Using the path to where you downloaded the certificate, connect to the server:
```
ssh -i "{path/to/your/kaypair.cer}" ec2-44-208-167-138.compute-1.amazonaws.com
```
### Connecting IDE (VSCode)

### Configure AWS
You will need to have an `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` generated, see this [getting started documentation](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-prereqs.html)
Complete the environmental variables needed in `.env.sample` and move to `.env`
run the following command:
```
aws configure
```
and interactualy populate the values as required:
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
```
### Restart postgres
```
sudo systemctl restart postgresql-12.service
```
