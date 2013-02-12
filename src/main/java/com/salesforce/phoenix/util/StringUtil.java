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
package com.salesforce.phoenix.util;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.util.Bytes;


public class StringUtil {
    // Masks to determine how many bytes are in each character
    // From http://tools.ietf.org/html/rfc3629#section-3
    private static final byte SPACE_UTF8 = 0x20;
    private static final int BYTES_1_MASK = 0xFF << 7; // 0xxxxxxx is a single byte char
    private static final int BYTES_2_MASK = 0xFF << 5; // 110xxxxx is a double byte char
    private static final int BYTES_3_MASK = 0xFF << 4; // 1110xxxx is a triple byte char
    private static final int BYTES_4_MASK = 0xFF << 3; // 11110xxx is a quadruple byte char

    private StringUtil() {
    }

    /** Replace instances of character ch in String value with String replacement */
    public static String replaceChar(String value, char ch, CharSequence replacement) {
        if (value == null)
            return null;
        int i = value.indexOf(ch);
        if (i == -1)
            return value; // nothing to do

        // we've got at least one character to replace
        StringBuilder buf = new StringBuilder(value.length() + 16); // some extra space
        int j = 0;
        while (i != -1) {
            buf.append(value, j, i).append(replacement);
            j = i + 1;
            i = value.indexOf(ch, j);
        }
        if (j < value.length())
            buf.append(value, j, value.length());
        return buf.toString();
    }

    /**
     * @return the replacement of all occurrences of src[i] with target[i] in s. Src and target are not regex's so this
     *         uses simple searching with indexOf()
     */
    public static String replace(String s, String[] src, String[] target) {
        assert src != null && target != null && src.length > 0 && src.length == target.length;
        if (src.length == 1 && src[0].length() == 1) {
            return replaceChar(s, src[0].charAt(0), target[0]);
        }
        if (s == null)
            return null;
        StringBuilder sb = new StringBuilder(s.length());
        int pos = 0;
        int limit = s.length();
        int lastMatch = 0;
        while (pos < limit) {
            boolean matched = false;
            for (int i = 0; i < src.length; i++) {
                if (s.startsWith(src[i], pos) && src[i].length() > 0) {
                    // we found a matching pattern - append the acculumation plus the replacement
                    sb.append(s.substring(lastMatch, pos)).append(target[i]);
                    pos += src[i].length();
                    lastMatch = pos;
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                // we didn't match any patterns, so move forward 1 character
                pos++;
            }
        }
        // see if we found any matches
        if (lastMatch == 0) {
            // we didn't match anything, so return the source string
            return s;
        }
        
        // apppend the trailing portion
        sb.append(s.substring(lastMatch));
        
        return sb.toString();
    }

    private static int getBytesInChar(byte b) throws UnsupportedEncodingException {
        int c = b & 0xff;
        if ((c & BYTES_1_MASK) == 0)
            return 1;
        if ((c & BYTES_2_MASK) == 0xC0)
            return 2;
        if ((c & BYTES_3_MASK) == 0xE0)
            return 3;
        if ((c & BYTES_4_MASK) == 0xF0)
            return 4;
        // Any thing else in the first byte is invalid
        throw new UnsupportedEncodingException("Undecodable byte: " + b);
    }

    public static int calculateUTF8Length(byte[] bytes, int offset, int length) throws UnsupportedEncodingException {
        int i = offset, endOffset = offset + length;
        length = 0;
        while (i < endOffset) {
            int charLength = getBytesInChar(bytes[i]);
            i += charLength;
            length++;
        }
        return length;
    }

    // Given an array of bytes containing encoding utf-8 encoded strings, the offset and a length
    // parameter, return the actual index into the byte array which would represent a substring
    // of <length> starting from the character at <offset>. We assume the <offset> is the start
    // byte of an UTF-8 character.
    public static int getByteLengthForUtf8SubStr(byte[] bytes, int offset, int length) throws UnsupportedEncodingException {
        int byteLength = 0;
        while(length > 0 && offset + byteLength < bytes.length) {
            int charLength = getBytesInChar(bytes[offset + byteLength]);
            byteLength += charLength;
            length--;
        }
        return byteLength;
    }

    public static boolean hasMultiByteChars(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c > 0x007F) {
                return true;
            }
        }
        return false;
    }

    public static int getFirstNonBlankCharIdxFromStart(byte[] string, int offset, int length)
            throws UnsupportedEncodingException {
        int i = offset;
        for ( ; i < offset + length; i++) {
            if ((getBytesInChar(string[i]) != 1 ||
                (getBytesInChar(string[i]) == 1 && SPACE_UTF8 < string[i] && string[i] != 0x7f))) {
                break;
            }
        }
        return i;
    }

    public static int getFirstNonBlankCharIdxFromEnd(byte[] string, int offset, int length) {
        int i = offset + length - 1;
        for ( ; i >= offset; i--) {
            int b = string[i] & 0xff;
            if (((b & BYTES_1_MASK) != 0) ||
                ((b & BYTES_1_MASK) == 0 && SPACE_UTF8 < b && b != 0x7f)) {
                break;
            }
        }
        return i;
    }

    // A toBytes function backed up HBase's utility function, but would accept null input, in which
    // case it returns an empty byte array.
    public static byte[] toBytes(String input) {
        if (input == null) {
            return ByteUtil.EMPTY_BYTE_ARRAY;
        }
        return Bytes.toBytes(input);
    }
}
