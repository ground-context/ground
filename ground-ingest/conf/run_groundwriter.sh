#!/bin/bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_92.jdk/Contents/Home

CLASSPATH=/Users/sreyashinag/Documents/workspace/sreyashi-ground-fork/ground-ingest/target/ground-ingest-0.1-SNAPSHOT-jar-with-dependencies.jar
GOBBLIN_FWDIR="$(cd `dirname $0`/..; pwd)"
FWDIR_CONF=$GOBBLIN_FWDIR/conf
GOBBLIN_LOG_DIR="$GOBBLIN_FWDIR/logs"
DEFAULT_CONFIG_FILE=$FWDIR_CONF/gobblin-standalone.properties
GOBBLIN_CUSTOM_CONFIG_FILE=$CUSTOM_CONFIG_FILE
GOBBLIN_JVM_FLAGS=$JVM_FLAGS
DEBUG_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1044
$JAVA_HOME/bin/java $DEBUG_OPTS -Xmx2g -Xms1g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+UseCompressedOops -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$GOBBLIN_LOG_DIR/ -Xloggc:$GOBBLIN_LOG_DIR/gobblin-gc.log -Dgobblin.logs.dir=$GOBBLIN_LOG_DIR -Dlog4j.configuration=file://$FWDIR_CONF/log4j-standalone.xml -cp $CLASSPATH -Dorg.quartz.properties=$FWDIR_CONF/quartz.properties $GOBBLIN_JVM_FLAGS edu.berkeley.ground.ingest.GroundWriter
