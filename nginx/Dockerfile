FROM nginx:latest
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*
COPY nginx.conf /etc/nginx/nginx.conf
