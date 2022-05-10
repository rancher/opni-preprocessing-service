FROM rancher/opni-python-base:3.8
WORKDIR /code
RUN zypper refresh
RUN zypper --non-interactive install  vim curl

COPY ./preprocessing-service/preprocess.py .
COPY ./preprocessing-service/masker.py .

CMD [ "python", "./preprocess.py" ]
# CMD [ "sleep", "5000000000" ]
