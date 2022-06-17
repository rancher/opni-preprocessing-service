FROM rancher/opni-python-base:3.8
WORKDIR /code

COPY ./preprocessing-service/preprocess.py .
COPY ./preprocessing-service/payload_pb2.py .
COPY ./preprocessing-service/masker.py .
RUN pip install protobuf==3.19.4

CMD [ "python", "./preprocess.py" ]
