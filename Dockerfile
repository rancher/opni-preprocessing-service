FROM rancher/opni-python-base:3.8

COPY ./preprocessing-service/ /app/
RUN pip install opni-aiops-apis==0.5.4
RUN chmod a+rwx -R /app
WORKDIR /app

CMD [ "python", "./preprocess.py" ]
