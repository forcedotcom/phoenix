/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
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
package com.salesforce.phoenix.logging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.time.StopWatch;

/**
 * Performance data logging  
 * @author mchohan
 */
public class PerformanceLog {
	private StopWatch stopWatch = null;	
	private static FileOutputStream fostream = null;

	public PerformanceLog(String startMessage) throws IOException {
		getStopWatch().start();
		instanceLog("START: " + startMessage);
	}
	
	public static void startLog() throws FileNotFoundException {
		getFileOutputStream();
	}
	
	public static void startLog(String fileName) throws FileNotFoundException {
		getFileOutputStream(fileName);
	}

	public static void stopLog() throws FileNotFoundException, IOException {
		getFileOutputStream().close();
		fostream = null;
	}

	public void stopStopWatch() throws IOException {
		getStopWatch().stop();
		instanceLog("STOP");		
	}
	
	public void stopStopWatch(String message) throws IOException {
		getStopWatch().stop();
		instanceLog("STOP: " + message);		
	}
	
	/**
	 * Log a message to persistent storage. Elapsed time since start is added.
	 * @param message
	 * @throws IOException
	 */
	public void instanceLog(String message) throws IOException {
		long elapsedMs = getStopWatch().getTime();
		String displayTime = elapsedMs < 1000 ? elapsedMs + " ms" : elapsedMs / 1000 + " sec";
		message = getDateTime() + " (" + displayTime + ") : " + message + "\n";
		System.out.println(message);
		getFileOutputStream().write(message.getBytes());
	}
	
	public static void log(String message) throws IOException {
		message = getDateTime() + ": " + message + "\n";
		System.out.println(message);
		getFileOutputStream().write(message.getBytes());
	}
	
	private static FileOutputStream getFileOutputStream() throws FileNotFoundException {
		return getFileOutputStream(null);
	}

	private static FileOutputStream getFileOutputStream(String fileName) throws FileNotFoundException {
		if (fostream == null) {
			String folderName = "results";
			File folder = new File(folderName);
			if (!folder.exists()) {
				folder.mkdir();
			}
			String generatedFileName = folderName
					+ "/"
					+ (fileName.endsWith("|") ? fileName.substring(0,
							fileName.length() - 1) : fileName) + ".txt";
			fostream = new FileOutputStream(generatedFileName);
		}

		return fostream;
	}
	
	private StopWatch getStopWatch() {
		if (stopWatch == null) {
			stopWatch = new StopWatch();
		}
		return stopWatch;
	}
	
	private final static String getDateTime() {
	    DateFormat df = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
	    return df.format(new Date());
	}
}
