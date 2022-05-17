FROM rancher/opni-python-base:3.8
WORKDIR /code

COPY ./preprocessing-service/preprocess.py .
COPY ./preprocessing-service/masker.py .

CMD [ "python", "./preprocess.py" ]
