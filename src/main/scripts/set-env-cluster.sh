# Run time parameters for the spark job in cluster mode
export SPARK_DRIVER_MEMORY="10g"
export SPARK_EXECUTOR_MEMORY="16g"
export SPARK_EXECUTOR_CORES =4
export SPARK_NUM_EXECUTORS =16
# Spark master to use (local vs yarn)
export SPARK_MASTER="yarn"

export JAVA_HOME= #java installed directory
export PATH =$JAVA_HOME/bin
export SPARK_HOME = #spark installed directory#
export PATH =$PATH:$SPARK_HOME/bin
