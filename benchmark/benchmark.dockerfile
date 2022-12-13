FROM python:3.11-slim

RUN groupadd styx \
    && useradd -m -d /usr/local/styx -g styx styx

USER styx

COPY --chown=styx:styx benchmark/requirements.txt /var/local/styx/
COPY --chown=styx:styx styx-package /var/local/styx-package/

ENV PATH="/usr/local/styx/.local/bin:${PATH}"

RUN pip install --upgrade pip \
    && pip install --user -r /var/local/styx/requirements.txt \
    && pip install --user ./var/local/styx-package/

WORKDIR /usr/local/styx

COPY --chown=styx:styx benchmark .

ENV PYTHONPATH /usr/local/styx

USER styx
CMD ["sanic", "app.app", "--host=0.0.0.0", "--port=5000"]

# default port
EXPOSE 5000