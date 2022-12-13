FROM python:3.10-slim

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

USER universalis

COPY --chown=universalis:universalis ingress/requirements.txt /var/local/universalis/
COPY --chown=universalis:universalis universalis-package /var/local/universalis-package/

ENV PATH="/usr/local/universalis/.local/bin:${PATH}"

RUN pip install --upgrade pip \
    && pip install --user -r /var/local/universalis/requirements.txt \
    && pip install --user ./var/local/universalis-package/

WORKDIR /usr/local/universalis

COPY --chown=universalis:universalis ingress ingress

COPY --chown=universalis:universalis ingress/start-ingress.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-ingress.sh

ENV PYTHONPATH /usr/local/universalis

USER universalis
CMD ["/usr/local/bin/start-ingress.sh"]

EXPOSE 8888