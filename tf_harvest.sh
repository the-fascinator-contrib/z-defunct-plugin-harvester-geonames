#!/bin/bash

. /opt/the-fascinator2/code/tf_env.sh

OS=`uname`
if [ "$OS" == "Darwin" ]; then
	TEST=`ps a | grep "java -jar start.jar"`
else
	TEST=`pgrep -l -f "java -jar start.jar"` 
fi

if [ "$TEST" != "" ]; then
	mvn exec:java
else
	echo "[ERROR] SOLR does not appear to be running"
fi

