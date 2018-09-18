#!/bin/bash

METRICS_DIR=/dbfs/spark-metrics
SPARK_CONF_DIR=/databricks/spark/conf
METRICS_PROPS=${SPARK_CONF_DIR}/metrics.properties
TEMP_METRICS_PROPS=${SPARK_CONF_DIR}/tmp.metrics.properties
echo "Copying metrics jar"
cp -f "$METRICS_DIR/azure-databricks-monitoring-0.9.jar" /mnt/driver-daemon/jars
echo "Copied metrics jar successfully"
cat "$METRICS_DIR/metrics.properties" <(echo) "$METRICS_PROPS" > "$TEMP_METRICS_PROPS"
mv "$TEMP_METRICS_PROPS" "$METRICS_PROPS"
echo "Merged metrics.properties successfully"
