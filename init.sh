 aws configure --profile $AWS_USER
 
 aws sns subscribe --topic-arn arn:aws:sns:us-east-1:470049585876:NewFetchObject --protocol lambda --notification-endpoint arn:aws:lambda:us-east-1:399143675412:function:openaq-realtime-function  --profile $AWS_USER