FROM rancher/opni-python-base:3.8
WORKDIR /code

COPY ./preprocessing-service/ /app/
RUN pip install protobuf==3.19.4
RUN chmod a+rwx -R /app
RUN pip install protobuf==3.19.4
WORKDIR /app

CMD [ "python", "./preprocess.py" ]
