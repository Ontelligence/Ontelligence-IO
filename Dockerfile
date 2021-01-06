FROM python:3.7

WORKDIR /app

ENV BUILD_DEPS="build-essential" \
    APP_DEPS="curl libpq-dev"

RUN apt-get update \
  && apt-get install -y ${BUILD_DEPS} ${APP_DEPS} --no-install-recommends \
  && rm -rf /var/lib/apt/lists/* \
  && rm -rf /usr/share/doc && rm -rf /usr/share/man \
  && apt-get purge -y --auto-remove ${BUILD_DEPS} \
  && apt-get clean

#ARG SSH_PRIVATE_KEY
#RUN mkdir -p /root/.ssh && umask 0077 && echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa \
#	&& git config --global url."git@github.com:".insteadOf https://github.com/ \
#	&& ssh-keyscan github.com >> ~/.ssh/known_hosts
#
#RUN pip install git+ssh://git@github.com/hamzaahmad-io/Ontelligence-IO.git

COPY . .
RUN pip install --editable .
RUN python main.py