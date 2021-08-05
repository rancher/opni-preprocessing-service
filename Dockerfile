FROM python:3.8-slim as base

FROM base as builder

RUN apt-get update \
    && apt-get install gcc -y \
    && apt-get clean

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install pre requisites
COPY ./preprocessing-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM base

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /code

# Add code
COPY ./preprocessing-service/preprocess.py .
COPY ./preprocessing-service/masker.py .

CMD [ "python", "./preprocess.py" ]
