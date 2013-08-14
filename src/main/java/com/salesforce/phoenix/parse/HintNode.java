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
package com.salesforce.phoenix.parse;

import java.util.HashMap;
import java.util.Map;

import com.salesforce.phoenix.util.SchemaUtil;


/**
 * Node representing optimizer hints in SQL
 */
public class HintNode {
    public static final HintNode EMPTY_HINT_NODE = new HintNode();
    
    public static final char SEPARATOR = ' ';
    public static final String PREFIX = "(";
    public static final String SUFFIX = ")";
    // Split on whitespace and parenthesis, keeping the parenthesis in the token array
    private static final String SPLIT_REGEXP = "\\s+|((?<=\\" + PREFIX + ")|(?=\\" + PREFIX + "))|((?<=\\" + SUFFIX + ")|(?=\\" + SUFFIX + "))";
    
    public enum Hint {
        /**
         * Forces a range scan to be used to process the query.
         */
        RANGE_SCAN,
        /**
         * Forces a skip scan to be used to process the query.
         */
        SKIP_SCAN,
        /**
         * Prevents the spawning of multiple threads during
         * query processing.
         */
        NO_INTRA_REGION_PARALLELIZATION,
        /**
        * Prevents the usage of indexes, forcing usage
        * of the data table for a query.
        */
       NO_INDEX,
       /**
       * Hint of the form INDEX(<table_name> <index_name>...)
       * to suggest usage of the index if possible. The first
       * usable index in the list of indexes will be choosen.
       * Table and index names may be surrounded by double quotes
       * if they are case sensitive.
       */
       INDEX;
    };

    private final Map<Hint,String> hints = new HashMap<Hint,String>();

    private HintNode() {
    }

    public HintNode(String hint) {
        // Split on whitespace or parenthesis. We do not need to handle escaped or
        // embedded whitespace/parenthesis, since we are parsing what will be HBase
        // table names which are not allowed to contain whitespace or parenthesis.
        String[] hintWords = hint.split(SPLIT_REGEXP);
        for (int i = 0; i < hintWords.length; i++) {
            String hintWord = hintWords[i];
            if (hintWord.isEmpty()) {
                continue;
            }
            try {
                Hint key = Hint.valueOf(hintWord.toUpperCase());
                String hintValue = "";
                if (i+1 < hintWords.length && PREFIX.equals(hintWords[i+1])) {
                    StringBuffer hintValueBuf = new StringBuffer(hint.length());
                    hintValueBuf.append(PREFIX);
                    i+=2;
                    while (i < hintWords.length && !SUFFIX.equals(hintWords[i])) {
                        hintValueBuf.append(SchemaUtil.normalizeIdentifier(hintWords[i++]));
                        hintValueBuf.append(SEPARATOR);
                    }
                    // Replace trailing separator with suffix
                    hintValueBuf.replace(hintValueBuf.length()-1, hintValueBuf.length(), SUFFIX);
                    hintValue = hintValueBuf.toString();
                }
                String oldValue = hints.put(key, hintValue);
                // Concatenate together any old value with the new value
                if (oldValue != null) {
                    hints.put(key, oldValue + hintValue);
                }
            } catch (IllegalArgumentException e) { // Ignore unknown/invalid hints
            }
        }
    }

    /**
     * Gets the value of the hint or null if the hint is not present.
     * @param hint the hint
     * @return the value specified in parenthesis following the hint or null
     * if the hint is not present.
     * 
     */
    public String getHint(Hint hint) {
        return hints.get(hint);
    }

    /**
     * Tests for the presence of a hint in a query
     * @param hint the hint
     * @return true if the hint is present and false otherwise
     */
    public boolean hasHint(Hint hint) {
        return hints.containsKey(hint);
    }
}
