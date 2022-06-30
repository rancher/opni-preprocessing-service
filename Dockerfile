FROM rancher/opni-python-base:3.8

COPY ./preprocessing-service/ /app/
RUN pip install protobuf==3.19.4
RUN pip install betterproto
RUN chmod a+rwx -R /app
WORKDIR /app

CMD [ "python", "./preprocess.py" ]
