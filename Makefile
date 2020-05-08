LIB_PATH=/home/yaoliu/src_code/protobuf-java-3.7.0.jar

all: clean
	mkdir bin
	mkdir bin/classes
	javac -classpath $(LIB_PATH) -d bin/classes/ src/keyValueStore/client/* src/keyValueStore/server/* src/keyValueStore/util/* src/keyValueStore/keyValue/* 

clean:
	rm -rf bin/ log/

