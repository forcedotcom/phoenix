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
package com.salesforce.phoenix.map.reduce.util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Class to read configs.
 * 
 */

public class ConfigReader
 {

	private String propertyFile = null;
	private boolean loaded = false;
	private static final Object _synObj = new Object();
	private Map<String, String> properties = new HashMap<String, String>();
	private Exception loadException = null;

	/**
	 * Retrieves singleton config objects from a hashmap of stored objects,
	 * creates these objects if they aren't in the hashmap.
	 */

	public ConfigReader(String propertyFile) {
		this.propertyFile = propertyFile;
	}

	public void load() throws Exception {
		if (loaded) {
			if (loadException != null) {
				throw new Exception(loadException);
			}
			return;
		}
		synchronized (_synObj) {
			if (!loaded) {
				try {
					String tmpFile = propertyFile.trim();
					if (tmpFile.endsWith(".properties")) {
						tmpFile = tmpFile
								.substring(0, tmpFile.lastIndexOf("."));
					}
					ResourceBundle resource = ResourceBundle.getBundle(tmpFile);
					Enumeration<String> enm = resource.getKeys();

					while (enm.hasMoreElements()) {
						String key = enm.nextElement();
						String value = resource.getString(key);
						properties.put(key, value);
					}
				} catch (Exception e) {
					System.err
							.println("Exception while loading the config.properties file :: "
									+ e.getMessage());
					loadException = e;
					loaded = true;
					throw e;
				}
				loaded = true;
			}
		}
	}

	public void addConfig(String key, String value) {
		try {
			load();
		} catch (Exception e) {
			System.err.println("ERROR :: " + e.getMessage());
		}
		properties.put(key, value);
	}

	public boolean hasConfig(String key) {
		try {
			load();
		} catch (Exception e) {
			System.err.println("ERROR :: " + e.getMessage());
		}
		return properties.containsKey(key);
	}

	public String getConfig(String key) throws Exception {
		load();
		return properties.get(key);
	}

	public Map<String, String> getAllConfigMap() throws Exception {
		load();
		return properties;
	}

}
