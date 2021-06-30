FROM python:3.8-slim
WORKDIR /code

# Install pre requisites
COPY ./preprocessing-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Add code
COPY ./preprocessing-service/preprocess.py .
COPY ./preprocessing-service/masker.py .

CMD [ "python", "./preprocess.py" ]
