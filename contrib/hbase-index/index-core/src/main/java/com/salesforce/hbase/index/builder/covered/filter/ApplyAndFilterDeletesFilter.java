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
package com.salesforce.hbase.index.builder.covered.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Only allow the 'latest' timestamp of each family:qualifier pair, ensuring that they aren't
 * covered by a previous delete. This is similar to some of the work the ScanQueryMatcher does to
 * ensure correct visibility of keys based on deletes.
 * <p>
 * No actual delete {@link KeyValue}s are allowed to pass through this filter - they are always
 * skipped.
 * <p>
 * Note there is a little bit of conceptually odd behavior (though it matches the HBase
 * specifications) around point deletes ({@link KeyValue} of type {@link Type#Delete}. These deletes
 * only apply to a single {@link KeyValue} at a single point in time - they essentially completely
 * 'cover' the existing {@link Put} at that timestamp. However, they don't 'cover' any other
 * keyvalues at older timestamps. Therefore, if there is a point-delete at ts = 5, and puts at ts =
 * 4, and ts = 5, we will only allow the put at ts = 4.
 * <p>
 * Expects {@link KeyValue}s to arrive in sorted order, with 'Delete' {@link Type} {@link KeyValue}s
 * ({@link Type#DeleteColumn}, {@link Type#DeleteFamily}, {@link Type#Delete})) before their regular
 * {@link Type#Put} counterparts.
 */
public class ApplyAndFilterDeletesFilter extends FilterBase {

  public static KeyValue END_ROW_KEY = KeyValue.createKeyValueFromKey(new byte[] { 127, 127, 127,
      127, 127, 127, 127 });
  private boolean done = false;
  List<byte[]>families;
  private KeyValue coveringDelete;
  public ApplyAndFilterDeletesFilter(Collection<byte[]> families){
    this.families = new ArrayList<byte[]>(families);
    Collections.sort(this.families, Bytes.BYTES_COMPARATOR);
  }
      
  
  private byte[] getNextFamily(byte[] family){
    int index = Collections.binarySearch(families, family, Bytes.BYTES_COMPARATOR);
    //doesn't match exactly, be we can find the right next match
    //this is pretty unlikely, but just incase
    if(index < 0){
      //the actual location of the next match
      index = -index -1;
    }else{
      //its an exact match for a family, so we get the next entry
      index = index +1;
    }
    //now we have the location of the next entry
    if(index >= families.size()){
      return null;
    }
    return  families.get(index);
  }
  
  @Override
  public void reset(){
    this.coveringDelete = null;
    this.done = false;
  }
  
  
  @Override
  public KeyValue getNextKeyHint(KeyValue peeked){
    KeyValue hint;
    //check to see if we have another column to seek
    byte[] nextFamily = getNextFamily(peeked.getFamily());
    if(nextFamily == null){
      //no known next family, so we can be completely done
      hint = END_ROW_KEY;
      this.done =true;
    }else{
      //there is a valid family, so we should seek to that
      hint = KeyValue.createFirstOnRow(peeked.getRow(), nextFamily, HConstants.EMPTY_BYTE_ARRAY);
    }
    
    return hint;
  }
  
  @Override
  public ReturnCode filterKeyValue(KeyValue next){
    //we marked ourselves done, but the END_ROW_KEY didn't manage to seek to the very last key
    if(this.done){
      return ReturnCode.SKIP;
    }

    switch (KeyValue.Type.codeToType(next.getType())) {
    /*
     * check for a delete to see if we can just replace this with a single delete; if its a family
     * delete, then we have deleted all columns and are definitely done with this family. This works
     * because deletes will always sort first, so we can be sure that if we see a delete, we can
     * skip everything else.
     */
    case DeleteFamily:
      // if its the delete of the entire column, then there are no more possible columns it
      // could be and we move onto the next column
    case DeleteColumn:
      return ReturnCode.SEEK_NEXT_USING_HINT;
    case Delete:
      // we are just deleting the single column value at this point.
      // therefore we just skip this entry and go onto the next one. The only caveat is that
      // we should still cover the next entry if this delete applies to the next entry, so we
      // have to keep around a reference to the KV to compare against the next valid entry
      this.coveringDelete = next;
      return ReturnCode.SKIP;
    default:
      // its definitely not a DeleteFamily, since we checked that already, so its a valid
      // entry and we can just add it, as long as it isn't directly covered by the previous
      // delete
      if (coveringDelete != null
          && coveringDelete.matchingColumn(next.getFamily(), next.getQualifier())) {
        // check to see if the match applies directly to this version
        if (coveringDelete.getTimestamp() == next.getTimestamp()) {
          /*
           * this covers this exact key. Therefore, we can skip this key. However, we can't discard
           * the covering delete because it might match the next put as well (in the case of having
           * the same put twice).
           */
          return ReturnCode.SKIP;
        }
      }
      // delete no longer applies, we are onto the next entry
      coveringDelete = null;
      return ReturnCode.INCLUDE;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("Server-side only filter, cannot be serialized!");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("Server-side only filter, cannot be deserialized!");
  }
}