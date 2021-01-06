# Ontelligence-IO

This is a sample description to be filled out later.

## Installation

Run the following to install:

```shell script
pip install /path/to/Ontelligence-IO
```

```shell script
pip install git+https://github.com/hamzaahmad-io/Ontelligence-IO.git
```


## Usage

```python
from ontelligence import *
```


## Dockerfile Example (Development)

```shell script
docker build -t ontelligence-io:v1 . && docker run -it ontelligence-io:v1
```


## Dockerfile Example (Production)

```dockerfile
FROM python:3.6 as stage
LABEL maintainer="Hamza Ahmad <hamza.ahmad@me.com>"

ARG SSH_PRIVATE_KEY

RUN mkdir -p /root/.ssh && umask 0077 && echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa \
	&& git config --global url."git@github.com:".insteadOf https://github.com/ \
	&& ssh-keyscan github.com >> ~/.ssh/known_hosts

RUN git clone git@github.com:hamzaahmad-io/Ontelligence-IO.git

##################################################

FROM python:3.7.4-slim-buster
LABEL maintainer="Hamza Ahmad <hamza.ahmad@me.com>"

COPY --from=stage "/Ontelligence-IO" "/opt/Ontelligence-IO"

WORKDIR /app

ENV BUILD_DEPS="build-essential" \
    APP_DEPS="curl libpq-dev"

RUN apt-get update \
  && apt-get install -y ${BUILD_DEPS} ${APP_DEPS} --no-install-recommends \
  && pip install -r requirements.txt \
  && pip install /opt/Ontelligence-IO \
  && rm -rf /var/lib/apt/lists/* \
  && rm -rf /usr/share/doc && rm -rf /usr/share/man \
  && apt-get purge -y --auto-remove ${BUILD_DEPS} \
  && apt-get clean

CMD ["echo", "done"]
```