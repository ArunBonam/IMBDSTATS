# Run time parameters for the spark job in local mode
export SPARK_DRIVER_MEMORY="4g"
export SPARK_EXECUTOR_MEMORY="4g"
export SPARK_EXECUTOR_CORES =2
export SPARK_NUM_EXECUTORS =4
export SPARK_MASTER="local[*]"
export spark.storage.memoryFraction=0
export JAVA_HOME= #java installed directory
export PATH =$JAVA_HOME/bin
export SPARK_HOME = #spark installed directory#
export PATH =$PATH:$SPARK_HOME/bin
