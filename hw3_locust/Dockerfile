FROM python:3.12.7-slim
RUN pip install --no-cache-dir locust
WORKDIR /app
COPY . /app
EXPOSE 8089
ENTRYPOINT ["locust"]