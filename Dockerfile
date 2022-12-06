FROM rancher/opni-python-base:3.8

COPY ./preprocessing-service/ /app/
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN chmod a+rwx -R /app
WORKDIR /app

CMD [ "python", "./preprocess.py"]
