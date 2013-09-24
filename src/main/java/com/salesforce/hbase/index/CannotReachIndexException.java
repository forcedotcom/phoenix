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
package com.salesforce.hbase.index;

import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;

/**
 * Exception thrown if we cannot successfully write to an index table.
 */
@SuppressWarnings("serial")
public class CannotReachIndexException extends Exception {

  /**
   * Cannot reach the index, but not sure of the table or the mutations that caused the failure
   * @param msg more description of what happened
   * @param cause original cause
   */
  public CannotReachIndexException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Failed to write the passed mutations to an index table for some reason.
   * @param targetTableName index table to which we attempted to write
   * @param mutations mutations that were attempted
   * @param cause underlying reason for the failure
   */
  public CannotReachIndexException(String targetTableName, List<Mutation> mutations, Exception cause) {
    super("Failed to make index update:\n\t table: " + targetTableName + "\n\t edits: " + mutations
        + "\n\tcause: " + cause == null ? "UNKNOWN" : cause.getMessage(), cause);
  }
}