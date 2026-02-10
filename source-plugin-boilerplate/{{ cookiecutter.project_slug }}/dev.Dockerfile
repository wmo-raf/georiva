# This a dev image for testing your plugin when installed into the georiva image
FROM georiva:latest AS base

FROM georiva:latest

ARG PLUGIN_BUILD_UID
ENV PLUGIN_BUILD_UID=${PLUGIN_BUILD_UID:-9999}
ARG PLUGIN_BUILD_GID
ENV PLUGIN_BUILD_GID=${PLUGIN_BUILD_GID:-9999}

# If we aren't building as the same user that owns all the files in the base
# image/installed plugins we need to chown everything first.
COPY --from=base --chown=$PLUGIN_BUILD_UID:$PLUGIN_BUILD_GID /georiva /georiva
RUN groupmod -g $PLUGIN_BUILD_GID georiva_docker_group && usermod -u $PLUGIN_BUILD_UID $DOCKER_USER

# Install your dev dependencies manually.
COPY --chown=$PLUGIN_BUILD_UID:$PLUGIN_BUILD_GID ./plugins/{{ cookiecutter.project_module }}/requirements/dev.txt /tmp/plugin-dev-requirements.txt
RUN . /georiva/venv/bin/activate && pip3 install -r /tmp/plugin-dev-requirements.txt

COPY --chown=$PLUGIN_BUILD_UID:$PLUGIN_BUILD_GID ./plugins/{{ cookiecutter.project_module }}/ $GEORIVA_PLUGIN_DIR/{{ cookiecutter.project_module }}/
RUN . /georiva/venv/bin/activate && /georiva/plugins/install_plugin.sh --folder $GEORIVA_PLUGIN_DIR/{{ cookiecutter.project_module }} --dev

USER $PLUGIN_BUILD_UID:$PLUGIN_BUILD_GID
ENV DJANGO_SETTINGS_MODULE='georiva.config.settings.dev'
CMD ["django-dev"]