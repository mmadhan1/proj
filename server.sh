#!/bin/bash +vx
LIB_PATH=$"/home/yaoliu/src_code/protobuf-java-3.7.0.jar"
#port
java -classpath bin/classes:$LIB_PATH src.server.ReplicaServers $1 $2 $3 $4
