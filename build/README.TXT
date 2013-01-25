SETUP
-----
Please set your JAVA_HOME and ANT_HOME environment variables before running build script. 
You can also modify default.properties file instead of setting environment variables.

Example on how to set environment variables:
export JAVA_HOME=/usr/lib/jvm/default-java
export ANT_HOME=/home/user/apache-ant-1.8.0

IMPORTANT ANT TARGETS
---------------------
all: Combines the following hbase and phoenix target
hbase: Sync and compiles HBase
phoenix: Compiles Phoenix and generates phoenix-client.jar and phoenix.jar (should be copied to HBase/lib folder)
clean: Cleans Phoenix project
javadoc: Generates Phoenix JavaDoc

EXAMPLE
---------
./ant all
