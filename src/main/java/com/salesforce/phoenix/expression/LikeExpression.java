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
package com.salesforce.phoenix.expression;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.StringUtil;


/**
 * 
 * Implementation for LIKE operation where the first child expression is the string
 * and the second is the pattern. The pattern supports '_' character for single 
 * character wildcard match and '%' for zero or more character match. where these
 * characters may be escaped by preceding them with a '\'.
 * 
 * Example: foo LIKE 'ab%' will match a row in which foo starts with 'ab'
 *
 * @author jtaylor
 * @since 0.1
 */
public class LikeExpression extends BaseCompoundExpression {
    private static final Logger logger = LoggerFactory.getLogger(LikeExpression.class);
    
    private static final String ZERO_OR_MORE = "\\E.*\\Q";
    private static final String ANY_ONE = "\\E.\\Q";
    
    public static String unescapeLike(String s) {
        return StringUtil.replace(s, StringUtil.LIKE_ESCAPE_SEQS, StringUtil.LIKE_UNESCAPED_SEQS);
    }

    /**
     * @return the substring of s for which we have a literal string
     *  that we can potentially use to set the start/end key, or null
     *  if there is none.
     */
    public static String getStartsWithPrefix(String s) {
        int i = indexOfWildcard(s);
        return i == -1 ? s : s.substring(0,i);
    }

    public static boolean hasWildcards(String s) {
        return indexOfWildcard(s) != -1;
    }
    
    /**
     * Replace unescaped '*' and '?' in s with '%' and '_' respectively
     * such that the returned string may be used in a LIKE expression.
     * Provides an alternate way of expressing a LIKE pattern which is
     * more friendly for wildcard matching when the source string is
     * likely to contain an '%' or '_' character.
     * @param s wildcard pattern that may use '*' for multi character
     * match and '?' for single character match, escaped by the backslash
     * character
     * @return replaced 
     */
    public static String wildCardToLike(String s) {
        s = StringUtil.escapeLike(s);
        StringBuilder buf = new StringBuilder();
        // Look for another unprotected * or ? in the middle
        int i = 0;
        int j = 0;
        while (true) {
            int pctPos = s.indexOf(StringUtil.MULTI_CHAR_WILDCARD, i);
            int underPos = s.indexOf(StringUtil.SINGLE_CHAR_WILDCARD, i);
            if (pctPos == -1 && underPos == -1) {
                return i == 0 ? s : buf.append(s.substring(i)).toString();
            }
            i = pctPos;
            if (underPos != -1 && (i == -1 || underPos < i)) {
                i = underPos;
            }
            
            if (i > 0 && s.charAt(i - 1) == '\\') {
                // If we found protection then keep looking
                buf.append(s.substring(j,i-1));
                buf.append(s.charAt(i));
            } else {
                // We found an unprotected % or _ in the middle
                buf.append(s.substring(j,i));
                buf.append(s.charAt(i) == StringUtil.MULTI_CHAR_WILDCARD ? StringUtil.MULTI_CHAR_LIKE : StringUtil.SINGLE_CHAR_LIKE);
            }
            j = ++i;
        }
    }
    
    public static int indexOfWildcard(String s) {
        // Look for another unprotected % or _ in the middle
        if (s == null) {
            return -1;
        }
        int i = 0;
        while (true) {
            int pctPos = s.indexOf(StringUtil.MULTI_CHAR_LIKE, i);
            int underPos = s.indexOf(StringUtil.SINGLE_CHAR_LIKE, i);
            if (pctPos == -1 && underPos == -1) {
                return -1;
            }
            i = pctPos;
            if (underPos != -1 && (i == -1 || underPos < i)) {
                i = underPos;
            }
            
            if (i > 0 && s.charAt(i - 1) == '\\') {
                // If we found protection then keep looking
                i++;
            } else {
                // We found an unprotected % or _ in the middle
                return i;
            }
        }
    }

    private static String toPattern(String s) {
        StringBuilder sb = new StringBuilder(s.length());

        // From the JDK doc: \Q and \E protect everything between them
        sb.append("\\Q");
        boolean wasSlash = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (wasSlash) {
                sb.append(c);
                wasSlash = false;
            } else if (c == StringUtil.SINGLE_CHAR_LIKE) {
                sb.append(ANY_ONE);
            } else if (c == StringUtil.MULTI_CHAR_LIKE) {
                sb.append(ZERO_OR_MORE);
            } else if (c == '\\') {
                wasSlash = true;
            } else {
                sb.append(c);
            }
        }
        sb.append("\\E");
        // Found nothing interesting
        return sb.toString();
    }

//    private static String fromPattern(String s) {
//        StringBuilder sb = new StringBuilder(s.length());
//
//        for (int i = 0; i < s.length(); i++) {
//            if (s.substring(i).startsWith("\\Q")) {
//                while (s.substring(i + "\\Q".length()).startsWith("\\E")) {
//                    sb.append(s.charAt(i++ + "\\Q".length()));
//                }
//                i+= "\\E".length();
//            }
//            if (s.charAt(i) == '.') {
//                if (s.charAt(i+1) == '*') {
//                    sb.append('%');
//                    i+=2;                    
//                } else {
//                    sb.append('_');
//                    i++;
//                }
//            }
//        }
//        return sb.toString();
//    }

    private Pattern pattern;
    
    public LikeExpression() {
    }

    public LikeExpression(List<Expression> children) {
        super(children);
        init();
    }
    
    public boolean startsWithWildcard() {
        return pattern != null && pattern.pattern().startsWith("\\Q\\E");
    }
    
    private void init() {
        Expression e = getPatternExpression();
        if (e instanceof LiteralExpression) {
            LiteralExpression patternExpression = (LiteralExpression)e;
            String value = (String)patternExpression.getValue();
            pattern = Pattern.compile(toPattern(value));
        }
    }

    private Expression getStrExpression() {
        return children.get(0);
    }

    private Expression getPatternExpression() {
        return children.get(1);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Pattern pattern = this.pattern;
        if (pattern == null) { // TODO: don't allow? this is going to be slooowwww
            if (!getPatternExpression().evaluate(tuple, ptr)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("LIKE is FALSE: pattern is null");
                }
                return false;
            }
            String value = (String)PDataType.VARCHAR.toObject(ptr, getPatternExpression().getColumnModifier());
            pattern = Pattern.compile(toPattern(value));
            if (logger.isDebugEnabled()) {
                logger.debug("LIKE pattern is expression: " + pattern.pattern());
            }
        }
        
        if (!getStrExpression().evaluate(tuple, ptr)) {
            if (logger.isDebugEnabled()) {
                logger.debug("LIKE is FALSE: child expression is null");
            }
            return false;
        }
        
        String value = (String)PDataType.VARCHAR.toObject(ptr, getStrExpression().getColumnModifier());
        boolean matched = pattern.matcher(value).matches();
        ptr.set(matched ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
        if (logger.isDebugEnabled()) {
            logger.debug("LIKE(value='" + value + "'pattern='" + pattern.pattern() + "' is " + matched);
        }
        return true;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    @Override
    public PDataType getDataType() {
        return PDataType.BOOLEAN;
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }

    public String getLiteralPrefix() {
        if (pattern == null) {
            return "";
        }
        String pattern = this.pattern.pattern();
        int fromIndex = "\\Q".length();
        return pattern.substring(fromIndex, pattern.indexOf("\\E", fromIndex));
    }

    public boolean endsWithOnlyWildcard() {
        if (pattern == null) {
            return false;
        }
        String pattern = this.pattern.pattern();
        String endsWith = ZERO_OR_MORE + "\\E";
        return pattern.endsWith(endsWith) && 
        pattern.lastIndexOf(ANY_ONE, pattern.length() - endsWith.length() - 1) == -1 &&
        pattern.lastIndexOf(ZERO_OR_MORE, pattern.length() - endsWith.length() - 1) == -1;
    }
    
    @Override
    public String toString() {
        return (children.get(0) + " LIKE " + children.get(1));
    }
}
