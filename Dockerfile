# Base image
FROM python:3.9-slim-buster
WORKDIR /app
ARG PROJECT_NAME
COPY . .
RUN ls -la / ; echo ${PROJECT_NAME}

RUN pip install -r requirements.txt && \
    pip install .

WORKDIR /app/osint_examples

CMD python3