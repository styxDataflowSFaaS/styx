FROM python:3-slim

COPY demo/requirements.txt /var/local/styx/
COPY styx-package /var/local/styx-package/

RUN groupadd styx \
    && useradd -m -d /usr/local/styx -g styx styx

RUN pip install --upgrade pip \
    && pip install --prefix=/usr/local -r /var/local/styx/requirements.txt \
    && pip install --prefix=/usr/local ./var/local/styx-package/

WORKDIR /usr/local/styx/demo

COPY --chown=styx demo .

ENV PYTHONPATH /usr/local/styx

USER styx
CMD ["sanic", "app.app", "--host=0.0.0.0", "--port=5000", "--workers=4"]

# default port
EXPOSE 5000