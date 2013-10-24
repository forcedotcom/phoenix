#!/bin/bash
############################################################################
# Copyright (c) 2013, Salesforce.com, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# Neither the name of Salesforce.com nor the names of its contributors may
# be used to endorse or promote products derived from this software without
# specific prior written permission.
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

# commandline-options
# -i                             CSV data file path in hdfs (mandatory)
# -s                             Phoenix schema name (mandatory if table is created without default phoenix schema name)
# -t                             Phoenix table name (mandatory)
# -sql                           Phoenix create table ddl path (mandatory)
# -zk                            Zookeeper IP:<port> (mandatory)
# -o                             Output directory path in hdfs (optional)
# -idx                           Phoenix index table name (optional, index support is yet to be added)
# -error                         Ignore error while reading rows from CSV ? (1 - YES | 0 - NO, defaults to 1) (optional)
# -help                          Print all options (optional)

current_dir=$(cd $(dirname $0);pwd)
phoenix_jar_path="$current_dir/../target"
phoenix_client_jar=$(find $phoenix_jar_path/phoenix-*-client.jar)

"$HADOOP_HOME"/bin/hadoop -cp "$phoenix_client_jar" com.salesforce.phoenix.map.reduce.CSVBulkLoader "$@"
