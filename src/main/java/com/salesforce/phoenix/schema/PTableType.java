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
package com.salesforce.phoenix.schema;


public enum PTableType {
    SYSTEM("s"), 
    USER("u"),
    VIEW("v"),
    INDEX("i"); 

    private final String serializedValue;
    
    private PTableType(String serializedValue) {
        this.serializedValue = serializedValue;
    }
    
    public String getSerializedValue() {
        return serializedValue;
    }
    
    private static final PTableType[] FROM_VALUE;
    private static final int FROM_VALUE_OFFSET;
    
    static {
        int minChar = Integer.MAX_VALUE;
        int maxChar = Integer.MIN_VALUE;
        for (PTableType type : PTableType.values()) {
            char c = type.getSerializedValue().charAt(0);
            if (c < minChar) {
                minChar = c;
            }
            if (c > maxChar) {
                maxChar = c;
            }
        }
        FROM_VALUE_OFFSET = minChar;
        FROM_VALUE = new PTableType[maxChar - minChar + 1];
        for (PTableType type : PTableType.values()) {
            FROM_VALUE[type.getSerializedValue().charAt(0) - minChar] = type;
        }
    }
    
    public static PTableType fromSerializedValue(String serializedValue) {
        if (serializedValue.length() == 1) {
            int i = serializedValue.charAt(0) - FROM_VALUE_OFFSET;
            if (i >= 0 && i < FROM_VALUE.length && FROM_VALUE[i] != null) {
                return FROM_VALUE[i];
            }
        }
        throw new IllegalArgumentException("Unable to PTableType enum for serialized value of '" + serializedValue + "'");
    }
    
    public static PTableType fromSerializedValue(byte serializedByte) {
        int i = serializedByte - FROM_VALUE_OFFSET;
        if (i >= 0 && i < FROM_VALUE.length && FROM_VALUE[i] != null) {
            return FROM_VALUE[i];
        }
        throw new IllegalArgumentException("Unable to PTableType enum for serialized value of '" + (char)serializedByte + "'");
    }
}
