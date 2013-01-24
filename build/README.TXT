SETUP
-----
Set your phoenix root and ant location in default.properties

IMPORTANT ANT TARGETS
---------------------
hbase: Sync and compiles HBase
phoenix: Compiles Phoenix and generates phoenix-client.jar and phoenix.jar (should be copied to HBase/lib folder)
all: Combines the above hbase and phoenix target
clean: Cleans Phoenix project
javadoc: Generates Phoenix JavaDoc

EXAMPLE
---------
./ant all
