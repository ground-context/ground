#!/bin/bash
CLASSPATH=/Users/sreyashinag/Documents/workspace/sreyashi-ground-fork/ground-ingest/target/ground-ingest-0.1-SNAPSHOT-jar-with-dependencies.jar
GOBBLIN_FWDIR="$(cd `dirname $0`/..; pwd)"
FWDIR_CONF=$GOBBLIN_FWDIR/conf
GOBBLIN_LOG_DIR="$GOBBLIN_FWDIR/logs"
DEFAULT_CONFIG_FILE=$FWDIR_CONF/gobblin-standalone.properties
GOBBLIN_CUSTOM_CONFIG_FILE=$CUSTOM_CONFIG_FILE
GOBBLIN_JVM_FLAGS=$JVM_FLAGS
$JAVA_HOME/bin/java -Xmx2g -Xms1g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+UseCompressedOops -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$GOBBLIN_LOG_DIR/ -Xloggc:$GOBBLIN_LOG_DIR/gobblin-gc.log -Dgobblin.logs.dir=$GOBBLIN_LOG_DIR -Dlog4j.configuration=file://$FWDIR_CONF/log4j-standalone.xml -cp $CLASSPATH -Dorg.quartz.properties=$FWDIR_CONF/quartz.properties $GOBBLIN_JVM_FLAGS gobblin.scheduler.SchedulerDaemon $DEFAULT_CONFIG_FILE $GOBBLIN_CUSTOM_CONFIG_FILE
