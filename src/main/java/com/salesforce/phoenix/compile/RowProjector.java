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
package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.*;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 * 
 * Class that manages a set of projected columns accessed through the zero-based
 * column index for a SELECT clause projection. The column index may be looked up
 * via the name using {@link #getColumnIndex(String)}.
 *
 * @author jtaylor
 * @since 0.1
 */
public class RowProjector {
    public static final RowProjector EMPTY_PROJECTOR = new RowProjector(Collections.<ColumnProjector>emptyList(),0, true);

    private final List<? extends ColumnProjector> columnProjectors;
    private final Map<String,Integer> reverseIndex;
    private final boolean allCaseSensitive;
    private final boolean someCaseSensitive;
    private final int estimatedSize;
    private final boolean isProjectEmptyKeyValue;
    
    public RowProjector(RowProjector projector, boolean isProjectEmptyKeyValue) {
        this(projector.getColumnProjectors(), projector.getEstimatedRowByteSize(), isProjectEmptyKeyValue);
    }
    /**
     * Construct RowProjector based on a list of ColumnProjectors.
     * @param columnProjectors ordered list of ColumnProjectors corresponding to projected columns in SELECT clause
     * aggregating coprocessor. Only required in the case of an aggregate query with a limit clause and otherwise may
     * be null.
     * @param estimatedRowSize 
     */
    public RowProjector(List<? extends ColumnProjector> columnProjectors, int estimatedRowSize, boolean isProjectEmptyKeyValue) {
        this.columnProjectors = Collections.unmodifiableList(columnProjectors);
        int position = columnProjectors.size();
        reverseIndex = Maps.newHashMapWithExpectedSize(position);
        boolean allCaseSensitive = true;
        boolean someCaseSensitive = false;
        for (--position; position >= 0; position--) {
            ColumnProjector colProjector = columnProjectors.get(position);
            allCaseSensitive &= colProjector.isCaseSensitive();
            someCaseSensitive |= colProjector.isCaseSensitive();
            reverseIndex.put(colProjector.getName(), position);
        }
        this.allCaseSensitive = allCaseSensitive;
        this.someCaseSensitive = someCaseSensitive;
        this.estimatedSize = estimatedRowSize;
        this.isProjectEmptyKeyValue = isProjectEmptyKeyValue;
    }
    
    public boolean isProjectEmptyKeyValue() {
        return isProjectEmptyKeyValue;
    }
    
    public List<? extends ColumnProjector> getColumnProjectors() {
        return columnProjectors;
    }
    
    public int getColumnIndex(String name) throws SQLException {
        if (!someCaseSensitive) {
            name = SchemaUtil.normalizeIdentifier(name);
        }
        Integer index = reverseIndex.get(name);
        if (index == null) {
            if (!allCaseSensitive && someCaseSensitive) {
                name = SchemaUtil.normalizeIdentifier(name);
                index = reverseIndex.get(name);
            }
            if (index == null) {
                throw new ColumnNotFoundException(name);
            }
        }
        return index;
    }
    
    public ColumnProjector getColumnProjector(int index) {
        return columnProjectors.get(index);
    }
 
    public int getColumnCount() {
        return columnProjectors.size();
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("[");
        for (ColumnProjector projector : columnProjectors) {
            buf.append(projector.getExpression());
            buf.append(',');
        }
        buf.setCharAt(buf.length()-1, ']');
        return buf.toString();
    }

    public int getEstimatedRowByteSize() {
        return estimatedSize;
    }
}