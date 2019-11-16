#!/bin/bash
MODE="local"
if [ $# -gt 0 ]; then
         MODE ="$1"
if [$MODE = "local" ]
then
    source ./set-env-local.sh
elif ["$1" = "cluster" ]
then
    source ./set-env-cluster.sh
else
    echo "Mode must be either local or cluster"
    exit 1
fi

LOG_FILE_NAME ="$3"
echo "Spark job starting in  $TENANT_NAME environment...."

spark-submit  \
--master ${SPARK_MASTER}  \
--deploy-mode client  \
--conf "spark.driver.extraJavaOptions=-Dlog.filename="${LOG_FILE_NAME}" -Dlog4j.configuration=file://"${driverPropFilePath}"" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration="${executorPropFile}"
--executor-memory "${SPARK_EXECUTOR_MEMORY}"
--driver-memory "${SPARK_DRIVER_MEMORY}"
--executor-cores "${SPARK_EXECUTOR_CORES}"
--num-executors "${numOfExecutors}"
--class