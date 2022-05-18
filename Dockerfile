FROM fjelltopp/ckan:base

USER root
RUN rm -rf /usr/lib/ckan/*
COPY ./ /usr/lib/ckan/
RUN rm -rf /usr/lib/ckan/ckan/ckan/pastertemplates/template/ckanext_+project_shortname+.egg-info
RUN cd /usr/lib/ckan/ && mkdir .venv && pipenv sync && \
    ln -s .venv venv
RUN chown -R ckan:ckan /usr/lib/ckan/
ENV PATH=${CKAN_VENV}/bin:${PATH}

RUN /usr/lib/ckan/bootstrap.sh

ENTRYPOINT ["/usr/lib/ckan/ckan-entrypoint.sh"]
#USER ckan
EXPOSE 5000
CMD ${CKAN_VENV}/bin/uwsgi --ini-paste ${CKAN_CONFIG}/ckan-uwsgi.ini



