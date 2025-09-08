{% macro external_table(table_name, gcs_uri) %}
create or replace external table {{ table_name }}
options(
  format = 'PARQUET',
  uris = ['{{ gcs_uri }}']
);
{% endmacro %}
