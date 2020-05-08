#!/bin/bash +vx
LIB_PATH=$"/home/yaoliu/src_code/protobuf-java-3.7.0.jar"
#port
java -classpath bin/classes:$LIB_PATH src.client.Client $1 $2
