Install the databricks CLI and configure it to connect to the cluster of choice

Edit the metrics properties to add the Log Analytics WorkspaceId and secret

Save!

Run commands below from this folder after building

databricks fs cp --overwrite ../target/azure-databricks-monitoring-0.9.jar dbfs:/spark-metrics/azure-databricks-monitoring-0.9.jar
databricks fs cp --overwrite metrics.properties dbfs:/spark-metrics/metrics.properties
databricks fs cp --overwrite spark-metrics.sh dbfs:/databricks/init/spark-metrics.sh

Restart the cluster and Log Analytics messages should show up.