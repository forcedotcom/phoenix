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

# Phoenix client jar. To generate new jars: $ mvn package -DskipTests
current_dir=$(cd $(dirname $0);pwd)
phoenix_jar_path="$current_dir/../target"
phoenix_client_jar=$(find $phoenix_jar_path/phoenix-*-client.jar)

if [ -z "$1" ] 
  then echo -e "Zookeeper not specified. \nUsage: sqlline.sh <zookeeper> <optional_sql_file> \nExample: \n 1. sqlline.sh localhost \n 2. sqlline.sh localhost ../examples/stock_symbol.sql";
  exit;
fi

if [ "$2" ] 
  then sqlfile="--run=$2";
fi

java -cp ".:$phoenix_client_jar" -Dlog4j.configuration=file:$current_dir/log4j.properties sqlline.SqlLine -d com.salesforce.phoenix.jdbc.PhoenixDriver -u jdbc:phoenix:$1 -n none -p none --color=true --fastConnect=false --silent=true --isolation=TRANSACTION_READ_COMMITTED $sqlfile
