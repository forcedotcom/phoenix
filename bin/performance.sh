#!/bin/bash
############################################################################
# Copyright (c) 2013, Salesforce.com, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#     Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#     Neither the name of Salesforce.com nor the names of its contributors may 
#     be used to endorse or promote products derived from this software without 
#     specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
############################################################################

# Note: This script is tested on Linux environment only. It should work on any Unix platform but is not tested.


# command line arguments
zookeeper=$1
rowcount=$2
table="performance_$2"

# helper variable and functions
ddl="ddl.sql"
data="data.csv"
qry="query.sql"

# Phoenix client jar. To generate new jars: $ mvn package -DskipTests
phoenix_jar_path="../target"
phoenix_client_jar=$(find $phoenix_jar_path/phoenix-*-client.jar)
testjar="$phoenix_jar_path/phoenix-*-tests.jar"

# HBase configuration folder path (where hbase-site.xml reside) for HBase/Phoenix client side property override
hbase_config_path="."

execute="java -cp "$hbase_config_path:$phoenix_client_jar" -Dlog4j.configuration=file:log4j.properties com.salesforce.phoenix.util.PhoenixRuntime -t $table $zookeeper "
timedexecute="time -p $execute"
function usage {
	echo "Performance script arguments not specified. Usage: performance.sh <zookeeper> <row count>"
	echo "Example: performance.sh localhost 100000"
	exit
} 
function queryex {
	echo ""
	echo "Query # $1"
	echo "Query: $2"
	echo "======================================================================================"
	echo $2>$qry;$timedexecute $qry
	echo "--------------------------------------------------------------------------------------"
}
function cleartempfiles {
	delfile $ddl
	delfile $data
	delfile $qry
}
function delfile {
	if [ -f $1 ]; then rm $1 ;fi;
}

# Create Table DDL
createtable="CREATE TABLE IF NOT EXISTS $table (HOST CHAR(2) NOT NULL,DOMAIN VARCHAR NOT NULL,
FEATURE VARCHAR NOT NULL,DATE DATE NOT NULL,USAGE.CORE BIGINT,USAGE.DB BIGINT,STATS.ACTIVE_VISITOR 
INTEGER CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, DATE)) 
SPLIT ON ('CSGoogle','CSSalesforce','EUApple','EUGoogle','EUSalesforce','NAApple','NAGoogle','NASalesforce');"

# generate and upsert data
clear
echo "Phoenix Performance Evaluation Script 1.0";echo "-----------------------------------------"
if [ -z "$2" ] 
then usage; fi;
echo ""; echo "Creating performance table..."
echo $createtable > $ddl; $execute "$ddl"
echo ""; echo "Upserting $rowcount rows...";echo "==========================="
echo "Generating data..."
java -jar $testjar $rowcount
echo ""
$timedexecute $data

# Write real,user,sys time on console for the following queries
queryex "1 - Count" "SELECT COUNT(1) FROM $table;"
queryex "2 - Group By First PK" "SELECT HOST FROM $table GROUP BY HOST;"
queryex "3 - Group By Second PK" "SELECT DOMAIN FROM $table GROUP BY DOMAIN;"
queryex "4 - Truncate + Group By" "SELECT TRUNC(DATE,'DAY') DAY FROM $table GROUP BY TRUNC(DATE,'DAY');"
queryex "5 - Filter + Count" "SELECT COUNT(1) FROM $table WHERE CORE<10;"

# clear temporary files
cleartempfiles
