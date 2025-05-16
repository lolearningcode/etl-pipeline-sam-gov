FROM public.ecr.aws/lambda/python:3.13

RUN mkdir -p /opt/python

RUN pip install requests boto3 -t /opt/python