#!/bin/sh

set -e

app_name=$1
script_name=$2


executor=5
executorcore=2
executormemory=2G
memoryoverhead=2048
drivermemory=2G


/opt/cloudera/parcels/SPARK2/bin/spark2-submit  \
                        --name app_name \
                        --master yarn \
                        --deploy-mode client \
                        --num-executors $executor \
                        --executor-cores $executorcore \
                        --executor-memory $executormemory \
                        --conf spark.sql.shuffle.partitions=128 \
                        --conf spark.default.parallelism=128 \
                        --conf spark.rdd.compress=false \
                        --conf spark.sql.orc.filterPushdown=true \
                        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
                        --conf spark.kryoserializer.buffer.max=512 \
                        --conf spark.yarn.am.cores=2 \
                        --conf spark.sql.hive.convertMetastoreOrc=false \
                        --driver-memory $drivermemory \
                        $script_name
