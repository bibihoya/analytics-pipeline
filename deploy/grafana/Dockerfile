FROM grafana/grafana:11.6.0
USER root
RUN grafana cli --pluginsDir /opt/grafana/plugins plugins install grafana-clickhouse-datasource 4.8.0
ENV GF_PATHS_PLUGINS=/opt/grafana/plugins
USER grafana
