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

import java.io.*;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.antlr.runtime.*;

import com.salesforce.phoenix.exception.PhoenixParserException;

/**
 * 
 * SQL Parser for Phoenix
 *
 * @author jtaylor
 * @since 0.1
 */
public class SQLParser {
    private static final ParseNodeFactory DEFAULT_NODE_FACTORY = new ParseNodeFactory();

    private final PhoenixSQLParser parser;

    public SQLParser(String query) {
        this(query,DEFAULT_NODE_FACTORY);
    }

    public SQLParser(String query, ParseNodeFactory factory) {
        PhoenixSQLLexer lexer;
        try {
            lexer = new PhoenixSQLLexer(new CaseInsensitiveReaderStream(new StringReader(query)));
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        CommonTokenStream cts = new CommonTokenStream(lexer);
        parser = new PhoenixSQLParser(cts);
        parser.setParseNodeFactory(factory);
    }

    public SQLParser(Reader queryReader, ParseNodeFactory factory) throws IOException {
        PhoenixSQLLexer lexer = new PhoenixSQLLexer(new CaseInsensitiveReaderStream(queryReader));
        CommonTokenStream cts = new CommonTokenStream(lexer);
        parser = new PhoenixSQLParser(cts);
        parser.setParseNodeFactory(factory);
    }

    public SQLParser(Reader queryReader) throws IOException {
        PhoenixSQLLexer lexer = new PhoenixSQLLexer(new CaseInsensitiveReaderStream(queryReader));
        CommonTokenStream cts = new CommonTokenStream(lexer);
        parser = new PhoenixSQLParser(cts);
        parser.setParseNodeFactory(DEFAULT_NODE_FACTORY);
    }

    /**
     * Parses the input as a series of semicolon-terminated SQL statements.
     * @throws SQLException 
     */
    public BindableStatement nextStatement(ParseNodeFactory factory) throws SQLException {
        try {
            parser.resetBindCount();
            parser.setParseNodeFactory(factory);
            BindableStatement statement = parser.nextStatement();
            return statement;
        } catch (RecognitionException e) {
            throw new PhoenixParserException(e, parser);
        } catch (UnsupportedOperationException e) {
            throw new SQLFeatureNotSupportedException(e);
        } catch (RuntimeException e) {
            throw new PhoenixParserException(e, parser);
        }
    }

    /**
     * Parses the input as a SQL select or upsert statement.
     * @throws SQLException 
     */
    public BindableStatement parseStatement() throws SQLException {
        try {
            BindableStatement statement = parser.statement();
            return statement;
        } catch (RecognitionException e) {
            throw new PhoenixParserException(e, parser);
        } catch (UnsupportedOperationException e) {
            throw new SQLFeatureNotSupportedException(e);
        } catch (RuntimeException e) {
            throw new PhoenixParserException(e, parser);
        }
    }

    /**
     * Parses the input as a SQL select statement.
     * Used only in tests
     * @throws SQLException 
     */
    public SelectStatement parseQuery() throws SQLException {
        try {
            SelectStatement statement = parser.query();
            return statement;
        } catch (RecognitionException e) {
            throw new PhoenixParserException(e, parser);
        }
    }

    /**
     * Parses the input as a SQL literal
     * @throws SQLException 
     */
    public LiteralParseNode parseLiteral() throws SQLException {
        try {
            LiteralParseNode literalNode = parser.literal();
            return literalNode;
        } catch (RecognitionException e) {
            throw new PhoenixParserException(e, parser);
        }
    }

    private static class CaseInsensitiveReaderStream extends ANTLRReaderStream {
        CaseInsensitiveReaderStream(Reader script) throws IOException {
            super(script);
        }

        @Override
        public int LA(int i) {
            if (i == 0) { return 0; // undefined
            }
            if (i < 0) {
                i++; // e.g., translate LA(-1) to use offset 0
            }

            if ((p + i - 1) >= n) { return CharStream.EOF; }
            return Character.toLowerCase(data[p + i - 1]);
        }
    }
}