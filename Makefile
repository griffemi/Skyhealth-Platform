.PHONY: init up down spark-bronze-batch spark-bronze-incremental spark-silver dbt-dev map map-timeslider test tf-init tf-plan tf-apply tf-destroy ansible-deploy ar-build-push
.RECIPEPREFIX := >

init:
>poetry install

up:
>docker-compose up -d

down:
>docker-compose down

spark-bronze-batch:
>poetry run python spark/jobs/bronze_openmeteo.py --mode backfill --range-start 1970-01-01

spark-bronze-incremental:
>poetry run python spark/jobs/bronze_openmeteo.py --mode incremental

spark-silver:
>poetry run python spark/jobs/silver_climate_daily_features.py

dbt-dev:
>poetry run dbt --project-dir dbt --profiles-dir dbt build

map:
>poetry run streamlit run apps/climate_map.py

map-timeslider:
>poetry run streamlit run apps/climate_map_timeslider.py

tf-init:
>cd infra/terraform && terraform init

tf-plan:
>cd infra/terraform && terraform plan

tf-apply:
>cd infra/terraform && terraform apply -auto-approve

tf-destroy:
>cd infra/terraform && terraform destroy -auto-approve

ansible-deploy:
>ansible-playbook -i infra/ansible/inventories/production/hosts.ini infra/ansible/site.yml

ar-build-push:
>docker build -t spark_submitter -f orchestrate/submitters/spark_submitter/Dockerfile .
>docker build -t dbt_builder -f orchestrate/submitters/dbt_builder/Dockerfile .
>docker build -t streamlit_app -f docker/streamlit.Dockerfile .
>docker build -t lightdash_app -f docker/lightdash.Dockerfile .
