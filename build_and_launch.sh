#!/bin/bash

SPARK_SUBMIT=$HOME/Downloads/spark-2.0.2-bin-hadoop2.7/bin/spark-submit
DRIVER_CLASS=$HOME/.m2/repository/org/postgresql/postgresql/9.4.1207.jre7/postgresql-9.4.1207.jre7.jar

# Compiles and packages code into JAR file for easy submission to spark
mvn package

# Submits job to spark
$SPARK_SUBMIT \
  --driver-class-path $DRIVER_CLASS \
  --class "AvailabilityPredictor" \
  --master local[4] target/citibike-project-1.0.jar
