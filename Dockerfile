# set base image (host OS)
FROM python:3.8-buster

RUN rm -f /etc/localtime
RUN ln -s /usr/share/zoneinfo/Asia/Jakarta /etc/localtime

# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY src/ .

# command to run on container start
CMD [ "python", "./json_to_parquet.py" ]