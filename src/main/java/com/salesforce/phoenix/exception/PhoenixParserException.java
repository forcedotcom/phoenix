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
package com.salesforce.phoenix.exception;

import java.sql.SQLSyntaxErrorException;

import org.antlr.runtime.*;

import com.salesforce.phoenix.parse.PhoenixSQLParser;


public class PhoenixParserException extends SQLSyntaxErrorException {
    private static final long serialVersionUID = 1L;

    public PhoenixParserException(Exception e, PhoenixSQLParser parser) {
        super(new SQLExceptionInfo.Builder(getErrorCode(e)).setRootCause(e)
                .setMessage(getErrorMessage(e, parser)).build().toString(),
                getErrorCode(e).getSQLState(), getErrorCode(e).getErrorCode(), e);
    }

    public static String getLine(RecognitionException e) {
        return Integer.toString(e.token.getLine());
    }

    public static String getColumn(RecognitionException e) {
        return Integer.toString(e.token.getCharPositionInLine() + 1);
    }

    public static String getTokenLocation(RecognitionException e) {
        return "line " + getLine(e) + ", column " + getColumn(e) + ".";
    }

    public static String getErrorMessage(Exception e, PhoenixSQLParser parser) {
        String[] tokenNames = parser.getTokenNames();
        String msg;
        if (e instanceof MissingTokenException) {
            MissingTokenException mte = (MissingTokenException)e;
            String tokenName;
            if (mte.expecting== Token.EOF) {
                tokenName = "EOF";
            } else {
                tokenName = tokenNames[mte.expecting];
            }
            msg = "Missing \""+ tokenName +"\" at "+ getTokenLocation(mte);
        } else if (e instanceof UnwantedTokenException) {
            UnwantedTokenException ute = (UnwantedTokenException)e;
            String tokenName;
            if (ute.expecting== Token.EOF) {
                tokenName = "EOF";
            } else {
                tokenName = tokenNames[ute.expecting];
            }
            msg = "Unexpected input. Expecting \"" + tokenName + "\", got \"" + ute.getUnexpectedToken().getText() 
                    + "\" at " + getTokenLocation(ute);
        } else if (e instanceof MismatchedTokenException) {
            MismatchedTokenException mte = (MismatchedTokenException)e;
            String tokenName;
            if (mte.expecting== Token.EOF) {
                tokenName = "EOF";
            } else {
                tokenName = tokenNames[mte.expecting];
            }
            msg = "Mismatched input. Expecting \"" + tokenName + "\", got \"" + mte.token.getText()
                    + "\" at " + getTokenLocation(mte);
        } else if (e instanceof RecognitionException){
            RecognitionException re = (RecognitionException) e;
            msg = "Encountered \"" + re.token.getText() + "\" at " + getTokenLocation(re);
        } else if (e instanceof UnknownFunctionException) {
            UnknownFunctionException ufe = (UnknownFunctionException) e;
            msg = "Unknown function: \"" + ufe.getFuncName() + "\".";
        } else {
            msg = e.getMessage();
        }
        return msg;
    }

    public static SQLExceptionCode getErrorCode(Exception e) {
        if (e instanceof MissingTokenException) {
            return SQLExceptionCode.MISSING_TOKEN;
        } else if (e instanceof UnwantedTokenException) {
            return SQLExceptionCode.UNWANTED_TOKEN;
        } else if (e instanceof MismatchedTokenException) {
            return SQLExceptionCode.MISMATCHED_TOKEN;
        } else if (e instanceof UnknownFunctionException) {
            return SQLExceptionCode.UNKNOWN_FUNCTION;
        } else {
            return SQLExceptionCode.PARSER_ERROR;
        }
    }
}
