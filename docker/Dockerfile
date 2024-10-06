FROM python:3.12-slim AS build

ENV APP_DIR=/opt/app
WORKDIR $APP_DIR


RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gcc \
    && curl -sSL https://install.python-poetry.org | POETRY_HOME=/usr/local/ python3 \
    && poetry config virtualenvs.in-project true \
    && apt-get purge --auto-remove -yqq \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /root/.cache/pip \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base


COPY poetry.lock pyproject.toml README.md $APP_DIR/
RUN poetry install --only main --no-root --no-interaction --no-ansi

COPY ecb_pipeline ecb_pipeline
RUN poetry build && .venv/bin/pip install dist/*.whl

FROM python:3.12-slim AS deploy

EXPOSE 8000
ENV USER=app-user
ENV APP_DIR=/opt/app
ENV ECB_LOCAL_DIR=/opt/data
ENV PATH="${APP_DIR}/.venv/bin:${PATH}"

RUN mkdir $ECB_LOCAL_DIR \
    && apt-get update \
    && apt-get purge --auto-remove -yqq \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /root/.cache/pip \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base \
    && groupadd -r $USER && useradd --no-log-init -rm -g $USER $USER \
    && chown -R $USER:$USER $ECB_LOCAL_DIR

WORKDIR $APP_DIR
COPY --from=build --chown=$USER:$USER ${APP_DIR}/.venv ${APP_DIR}/.venv

USER $USER
CMD ["uvicorn", "ecb_pipeline.app:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]