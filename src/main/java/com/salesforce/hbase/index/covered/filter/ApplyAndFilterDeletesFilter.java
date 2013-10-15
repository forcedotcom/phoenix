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
package com.salesforce.hbase.index.covered.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.FilterBase;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;

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

  private boolean done = false;
  List<ImmutableBytesPtr> families;
  private final DeleteTracker coveringDelete = new DeleteTracker();
  private Hinter currentHint;
  private DeleteColumnHinter columnHint = new DeleteColumnHinter();
  private DeleteFamilyHinter familyHint = new DeleteFamilyHinter();
  
  /**
   * Setup the filter to only include the given families. This allows us to seek intelligently pass
   * families we don't care about.
   * @param families
   */
  public ApplyAndFilterDeletesFilter(Set<ImmutableBytesPtr> families) {
    this.families = new ArrayList<ImmutableBytesPtr>(families);
    Collections.sort(this.families);
  }
      
  
  private ImmutableBytesPtr getNextFamily(ImmutableBytesPtr family) {
    int index = Collections.binarySearch(families, family);
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
    this.coveringDelete.reset();
    this.done = false;
  }
  
  
  @Override
  public KeyValue getNextKeyHint(KeyValue peeked){
    return currentHint.getHint(peeked);
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue next) {
    // we marked ourselves done, but the END_ROW_KEY didn't manage to seek to the very last key
    if (this.done) {
      return ReturnCode.SKIP;
    }

    switch (KeyValue.Type.codeToType(next.getType())) {
    /*
     * DeleteFamily will always sort first because those KVs (we assume) don't have qualifiers (or
     * rather are null). Therefore, we have to keep a hold of all the delete families until we get
     * to a Put entry that is covered by that delete (in which case, we are done with the family).
     */
    case DeleteFamily:
      // track the family to delete. If we are updating the delete, that means we have passed all
      // kvs in the last column, so we can safely ignore the last deleteFamily, and just use this
      // one
      this.coveringDelete.deleteFamily = next;
      return ReturnCode.SKIP;
    case DeleteColumn:
      // similar to deleteFamily, all the newer deletes/puts would have been seen at this point, so
      // we can safely replace the more recent delete column with the more recent one
      this.coveringDelete.deleteColumn = next;
      return ReturnCode.SKIP;
    case Delete:
      // we are just deleting the single column value at this point.
      // therefore we just skip this entry and go onto the next one. The only caveat is that
      // we should still cover the next entry if this delete applies to the next entry, so we
      // have to keep around a reference to the KV to compare against the next valid entry
      this.coveringDelete.pointDelete = next;
      return ReturnCode.SKIP;
    default:
      // no covering delete or it doesn't match this family.
      if (coveringDelete.empty() || !coveringDelete.matchingFamily(next)) {
        // we must be onto a new column family, so all the old markers are now defunct - we can
        // throw them out and track the next ones we find instead
        this.coveringDelete.reset();
        return ReturnCode.INCLUDE;
      }

      long nextTs = next.getTimestamp();
      // delete family applies, so we can skip everything else in the family after this
      if (coveringDelete.deleteFamily != null
          && coveringDelete.deleteFamily.getTimestamp() >= nextTs) {
        // hint to the next family
        this.currentHint = familyHint;
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }

      if (coveringDelete.deleteColumn != null
          && coveringDelete.deleteColumn.getTimestamp() >= nextTs) {
        // hint to the next column
        this.currentHint = columnHint;
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }

      // point deletes only apply to the exact KV that they reference, so we only need to ensure
      // that the timestamp matches exactly. Because we sort by timestamp first, either the next
      // keyvalue has the exact timestamp or is an older (smaller) timestamp, and we can allow that
      // one.
      if (coveringDelete.pointDelete != null) {
        if (coveringDelete.pointDelete.getTimestamp() == nextTs) {
          return ReturnCode.SKIP;
        }
        // clear the point delete since the TS must not be matching
        coveringDelete.pointDelete = null;
      }

    }

    // none of the deletes matches, we are done
    return ReturnCode.INCLUDE;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("Server-side only filter, cannot be serialized!");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("Server-side only filter, cannot be deserialized!");
  }

  /**
   * Get the next hint for a given peeked keyvalue
   */
  interface Hinter {
    public abstract KeyValue getHint(KeyValue peek);
  }

  /**
   * Entire family has been deleted, so either seek to the next family, or if none are present in
   * the original set of families to include, seek to the "last possible key"(or rather our best
   * guess) and be done.
   */
  class DeleteFamilyHinter implements Hinter {

    @Override
    public KeyValue getHint(KeyValue peeked) {
      // check to see if we have another column to seek
      ImmutableBytesPtr nextFamily =
          getNextFamily(new ImmutableBytesPtr(peeked.getBuffer(), peeked.getFamilyOffset(),
              peeked.getFamilyLength()));
      if (nextFamily == null) {
        // no known next family, so we can be completely done
        done = true;
        return KeyValue.LOWESTKEY;
      }
        // there is a valid family, so we should seek to that
      return KeyValue.createFirstOnRow(peeked.getRow(), nextFamily.copyBytesIfNecessary(),
        HConstants.EMPTY_BYTE_ARRAY);
    }

  }

  /**
   * Hint the next column-qualifier after the given keyvalue. We can't be smart like in the
   * ScanQueryMatcher since we don't know the columns ahead of time.
   */
  class DeleteColumnHinter implements Hinter {

    @Override
    public KeyValue getHint(KeyValue kv) {
      return KeyValue.createLastOnRow(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
        kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getBuffer(),
        kv.getQualifierOffset(), kv.getQualifierLength());
    }
  }

  class DeleteTracker {

    public KeyValue deleteFamily;
    public KeyValue deleteColumn;
    public KeyValue pointDelete;

    public void reset() {
      this.deleteFamily = null;
      this.deleteColumn = null;
      this.pointDelete = null;

    }

    /**
     * Check to see if any of the deletes match the family of the keyvalue
     * @param next
     * @return <tt>true</tt> if any of current delete applies to this family
     */
    public boolean matchingFamily(KeyValue next) {
      if (deleteFamily != null && deleteFamily.matchingFamily(next)) {
        return true;
      }
      if (deleteColumn != null && deleteColumn.matchingFamily(next)) {
        return true;
      }
      if (pointDelete != null && pointDelete.matchingFamily(next)) {
        return true;
      }
      return false;
    }

    /**
     * @return <tt>true</tt> if no delete has been set
     */
    public boolean empty() {
      return deleteFamily == null && deleteColumn == null && pointDelete == null;
    }
  }
}