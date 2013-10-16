/*******************************************************************************
* Copyright (c) 2013, Salesforce.com, Inc.
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice,
* this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
* this list of conditions and the following disclaimer in the documentation
* and/or other materials provided with the distribution.
* Neither the name of Salesforce.com nor the names of its contributors may
* be used to endorse or promote products derived from this software without
* specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
* ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
* FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
* DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
* SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
* CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
* OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
* OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
******************************************************************************/
package com.salesforce.phoenix.map.reduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.io.Closeables;
import com.salesforce.phoenix.map.reduce.util.ConfigReader;
import com.salesforce.phoenix.util.PhoenixRuntime;

public class CSVBulkLoader {
	
	static FileWriter wr = null;
	static BufferedWriter bw = null;
	static boolean isDebug = false; //Set to true, if you need to log the bulk-import time.
	static ConfigReader systemConfig = null;

	static String schemaName = "";
	static String tableName = "";
	static String idxTable = "";
	static String createPSQL[] = null;
	static String skipErrors = null;
	static String zookeeperIP = null;

	static{
		/** load the log-file writer, if debug is true **/
		if(isDebug){
			try {
			    wr = new FileWriter("phoenix-bulk-import.log", false);
			    bw = new BufferedWriter(wr);
			} catch (IOException e) {
			    System.err.println("Error preparing writer for log file :: " + e.getMessage());
			}
		}

		/** load the Map-Reduce configs **/
		try {
			systemConfig = new ConfigReader("csv-bulk-load-config.properties");
		} catch (Exception e) {
			System.err.println("Exception occurred while reading config properties");
			System.err.println("The bulk loader will run slower than estimated");
		}
	}
	
	/**
	 * -i		CSV data file path in hdfs
	 * -s		Phoenix schema name
	 * -t		Phoenix table name
	 * -sql  	Phoenix create table sql path (1 SQL statement per line)
	 * -zk		Zookeeper IP:<port>
	 * -o		Output directory path in hdfs (Optional)
	 * -idx  	Phoenix index table name (Optional)
	 * -error    	Ignore error while reading rows from CSV ? (1 - YES/0 - NO, defaults to 1) (OPtional)
	 * -help	Print all options (Optional)
	 */

	@SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception{
		
		String inputFile = null;
		String outFile = null;

		Options options = new Options();
		options.addOption("i", true, "CSV data file path");
		options.addOption("o", true, "Output directory path");
		options.addOption("s", true, "Phoenix schema name");
		options.addOption("t", true, "Phoenix table name");
		options.addOption("idx", true, "Phoenix index table name");
		options.addOption("zk", true, "Zookeeper IP:<port>");
		options.addOption("sql", true, "Phoenix create table sql path");
		options.addOption("error", true, "Ignore error while reading rows from CSV ? (1 - YES/0 - NO, defaults to 1)");
		options.addOption("help", false, "All options");
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse( options, args);
		
		if(cmd.hasOption("help")){
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "help", options );
			System.exit(0);
		}
		
		String parser_error = "ERROR while parsing arguments. ";
		//CSV input, table name, sql and zookeeper IP  are mandatory fields
		if(cmd.hasOption("i")){
			inputFile = cmd.getOptionValue("i");
		}else{
			System.err.println(parser_error + "Please provide CSV file input path");
			System.exit(0);
		}
		if(cmd.hasOption("t")){
			tableName = cmd.getOptionValue("t");
		}else{
			System.err.println(parser_error + "Please provide Phoenix table name");
			System.exit(0);
		}
		if(cmd.hasOption("sql")){
			String sqlPath = cmd.getOptionValue("sql");
			createPSQL = getCreatePSQLstmts(sqlPath);
		}else{
			System.err.println(parser_error + "Please provide Phoenix sql");
			System.exit(0);
		}
		if(cmd.hasOption("zk")){
			zookeeperIP = cmd.getOptionValue("zk");
		}else{
			System.err.println(parser_error + "Please provide Zookeeper address");
			System.exit(0);
		}

		if(cmd.hasOption("o")){
			outFile = cmd.getOptionValue("o");
		}else{
			outFile = "phoenix-output-dir";
		}
		if(cmd.hasOption("s")){
			schemaName = cmd.getOptionValue("s");
		}
		if(cmd.hasOption("idx")){
			idxTable = cmd.getOptionValue("idx");
		}
		if(cmd.hasOption("error")){
			skipErrors = cmd.getOptionValue("error");
		}else{
			skipErrors = "1";
		}
		
		log("[TS - START] :: " + new Date() + "\n");

		Path inputPath = new Path(inputFile);
		Path outPath = new Path(outFile);
		
		//Create the Phoenix table in HBase
		for(String s : createPSQL){
			if(s == null || s.trim().length() == 0)
				continue;
				createPTable(s);
		}
		
		log("[TS - Table created] :: " + new Date() + "\n");

		Configuration conf = new Configuration();
		loadMapRedConfigs(conf);
		
		Job job = new Job(conf, "MapReduce - Phoenix bulk import");
		job.setJarByClass(MapReduceJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		job.setMapperClass(MapReduceJob.PhoenixMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		
		SchemaMetrics.configureGlobally(conf);

		String dataTable = schemaName.toUpperCase() + "." + tableName.toUpperCase();
		HTable hDataTable = new HTable(conf, dataTable);
		
		// Auto configure partitioner and reducer according to the Main Data table
	    	HFileOutputFormat.configureIncrementalLoad(job, hDataTable);

    		job.waitForCompletion(true);
	    
		log("[TS - M-R HFile generated..Now dumping to HBase] :: " + new Date() + "\n");
		
    		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    		loader.doBulkLoad(new Path(outFile), hDataTable);
	    
		log("[TS - FINISH] :: " + new Date() + "\n");
		if(isDebug) bw.close();
		
	}
	
	public static void createPTable(String stmt) {
		
		Connection conn = null;
		PreparedStatement statement;

		try {
			conn = DriverManager.getConnection(getUrl(), "", "");
			statement = conn.prepareStatement(stmt);
			statement.execute();
			conn.commit();
		} catch (Exception e) {
			System.err.println("Error creating the table :: " + e.getMessage());
		} finally{
			try {
				conn.close();
			} catch (Exception e) {
				System.err.println("Failed to close connection :: " + e.getMessage());
			}
		}
		
	}
	
	private static String getUrl() {
        	return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zookeeperIP;
    	}
	
	private static void loadMapRedConfigs(Configuration conf){

		conf.set("IGNORE.INVALID.ROW", skipErrors);
		conf.set("schemaName", schemaName);
		conf.set("tableName", tableName);
		conf.set("zk", zookeeperIP);
		if(createPSQL[0] != null) conf.set("createTableSQL", createPSQL[0]);
		if(createPSQL[1] != null) conf.set("createIndexSQL", createPSQL[1]);
		
		//Load the other System-Configs
		try {
			
			Map<String, String> configs = systemConfig.getAllConfigMap();
			
			if(configs.containsKey("mapreduce.map.output.compress")){
				String s = configs.get("mapreduce.map.output.compress");
				if(s != null && s.trim().length() > 0)
					conf.set("mapreduce.map.output.compress", s);
			}
			
			if(configs.containsKey("mapreduce.map.output.compress.codec")){
				String s = configs.get("mapreduce.map.output.compress.codec");
				if(s != null && s.trim().length() > 0)
					conf.set("mapreduce.map.output.compress.codec", s);
			}
			
			if(configs.containsKey("io.sort.record.percent")){
				String s = configs.get("io.sort.record.percent");
				if(s != null && s.trim().length() > 0)
					conf.set("io.sort.record.percent", s);	
			}
				
			if(configs.containsKey("io.sort.factor")){
				String s = configs.get("io.sort.factor");
				if(s != null && s.trim().length() > 0)
					conf.set("io.sort.factor", s);
			}
			
			if(configs.containsKey("mapred.tasktracker.map.tasks.maximum")){
				String s = configs.get("mapred.tasktracker.map.tasks.maximum");
				if(s != null && s.trim().length() > 0)
					conf.set("mapred.tasktracker.map.tasks.maximum", s);
			}
				
		} catch (Exception e) {
			System.err.println("Error loading the configs :: " + e.getMessage());
			System.err.println("The bulk loader will run slower than estimated");
		}
	}
	
	private static String[] getCreatePSQLstmts(String path){
		
	    BufferedReader br = null;
		try {
			FileReader file = new FileReader(path);
			br = new BufferedReader(file);
			//Currently, we can have at-most 2 SQL statements - 1 for create table and 1 for index
			String[] sb = new String[2];
			String line;
			for(int i = 0; i < 2 && (line = br.readLine()) != null ; i++){
				sb[i] = line;
			}
			return sb;
			
		} catch (IOException e) {
			System.err.println("Error reading the file :: " + path + ", " + e.getMessage());
		} finally {
		    if (br != null) Closeables.closeQuietly(br);
		}
		return null;
	}
	
	private static void log(String msg){
		if(isDebug){
			try {
				bw.write(msg);
			} catch (IOException e) {
				System.err.println("Error logging the statement :: " + msg);
			}
		}
	}
}
