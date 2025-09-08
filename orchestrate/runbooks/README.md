# Runbooks

Cloud Scheduler triggers Cloud Run Jobs which submit Spark jobs or run dbt builds. The `spark_submitter` job starts the master VM if needed, resizes the worker MIG, posts to the Spark master REST endpoint, and shuts the cluster down after completion. `dbt_builder` runs `dbt build --target prod`.
