# Spark Application to extract IMDB listed movies and Persons collaborated


Project Structure:

MovieStatistics
     |
     |_ __ _ src
              |_ _ _ _ main
              |          |_ _ _ resources(contains all the datasets)
              |          |_ _ _  scripts (contains scripts to run spark jobs)
              |          |_ _ _ scala (contains main class and a UDF)
              |          |_ _ _ conf(required configuration parameters)
              |     
              |_ _ _ _ _ _ scala
                             |_ _ _ _ _UdfTest(contains test cases for this project)
                             
                             
Versions used :

1) Java 1.8 
2) Scala 2.12 
3) Spark core,sql v2.4
4) Scala test 3.0.8
5) typesafe library 1.2.1 (to handle configurations )

Prerequisites:

1)Set spark and Java paths in bash_profile
2)If not set ,source the environment files avaialable in src/main/scripts 
  If running on  local machine -- source set-env-local.sh
  If running on  cluster       -- source set-env-cluster.sh
  
Running Instructions:

1)Fat jar is avaialble in  the location : src/target/scala-2.12/MovieStatistics-assembly-1.0.jar
2)It can be run two ways- submitting the spark job manually or running the shell script: src/main/scripts/run-spark-job.sh
3) Before running make sure above mentioned  environment files were set  :local or cluster

From the project root folder ,Manual triggering :

spark-submit  \
--master local  \  (replace with yarn while running in cluster)
--deploy-mode client  \
--executor-memory "4g" --driver-memory "2g"  \
--executor-cores "3" --num-executors "4" \
--class ImdbInfo \
--verbose \
target/scala-2.12/moviestatistics_2.12-1.0.jar 


Triggering through shell script:

From project root folder:
sh src/main/srcpits/run_spark_job.sh local <logfilename>.log

Executor memory,number of cores ,num of executors mentioned in the above command should be changed
according to cluster size and no of cores avaialable in each node.

I used  memories while running in my local machine (16 core machine --4 cores left out for other processes).
All these parameters are configurable in set-env-cluster.sh and set-env-local.sh


TestCases are available in src/main/test/scala/MovieStatsTest. This class can be run standalone.


Missing piece of code:Logging .Will add it soon.

Final Resultant data: 











             
