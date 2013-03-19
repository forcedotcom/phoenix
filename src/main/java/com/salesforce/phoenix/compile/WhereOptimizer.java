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

import static com.salesforce.phoenix.query.QueryConstants.SEPARATOR_BYTE;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.expression.function.FunctionExpression.KeyFormationDirective;
import com.salesforce.phoenix.expression.function.ScalarFunction;
import com.salesforce.phoenix.expression.visitor.TraverseAllExpressionVisitor;
import com.salesforce.phoenix.expression.visitor.TraverseNoExpressionVisitor;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import com.salesforce.phoenix.util.*;

/**
 *
 * Class that pushes row key expressions from the where clause to form the start/stop
 * key of the scan and removes the expressions from the where clause when possible.
 *
 * @author jtaylor
 * @since 0.1
 */
public class WhereOptimizer {

    private WhereOptimizer() {
    }

    /**
     * Pushes row key expressions from the where clause into the start/stop key of the scan.
     * @param context the shared context during query compilation
     * @param whereClause the where clause expression
     * @return the new where clause with the key expressions removed
     */
    public static Expression pushKeyExpressionsToScan(StatementContext context, Expression whereClause) {
        return pushKeyExpressionsToScan(context, whereClause, null);
    }

    // For testing so that the extractedNodes can be verified
    public static Expression pushKeyExpressionsToScan(StatementContext context, Expression whereClause, Set<Expression> extractNodes) {
        if (whereClause == null) {
            context.setScanKey(ScanKey.EVERYTHING_SCAN_KEY);
            return whereClause;
        }
        if (whereClause == LiteralExpression.FALSE_EXPRESSION) {
            context.setScanKey(ScanKey.DEGENERATE_SCAN_KEY);
            return null;
        }
        KeyExpressionVisitor visitor = new KeyExpressionVisitor(context);
        KeyExpressionVisitor.KeyParts compKeyExpr = whereClause.accept(visitor);

        if (compKeyExpr == null) {
            context.setScanKey(ScanKey.EVERYTHING_SCAN_KEY);
            return whereClause;
        }
        // If a parameter is bound to null (as will be the case for calculating ResultSetMetaData and
        // ParameterMetaData), this will be the case. It can also happen for an equality comparison
        // for unequal lengths.
        if (compKeyExpr == KeyExpressionVisitor.DEGENERATE_KEY_EXPRESSION) {
            context.setScanKey(ScanKey.DEGENERATE_SCAN_KEY);
            return null;
        }

        KeyExpressionVisitor.KeyPart keyExpr = null;
        // Calculate the max length of a key (each part must currently be of a fixed width)
        // TODO: Single table for now
        TableRef tableRef = context.getResolver().getTables().get(0);
        PTable table = tableRef.getTable();
        if (extractNodes == null) {
            extractNodes = new HashSet<Expression>(table.getPKColumns().size());
        }

        int maxKeyLength = SchemaUtil.estimateKeyLength(table);
        TrustedByteArrayOutputStream startKey = null, stopKey = null;
        try {
            startKey = new TrustedByteArrayOutputStream(maxKeyLength);
            stopKey = new TrustedByteArrayOutputStream(maxKeyLength);
            boolean lastUpperInclusive = false;
            boolean lastLowerInclusive = false;
            boolean lastUpperVarLength = false;
            boolean lastLowerVarLength = false;
            RowKeySchemaBuilder schema = new RowKeySchemaBuilder();
            int lowerPosCount = 0;
            int upperPosCount = 0;
            int pkPos = -1;
            boolean isFullyQualifiedKey = true;
            List<List<KeyRange>> cnf = new ArrayList<List<KeyRange>>();
            List<PColumn> pkColumns = table.getPKColumns();
            boolean useSkipScan = false;
            boolean hasRangeKey = false;
            // Concat byte arrays of literals to form scan start key
            for (KeyExpressionVisitor.KeySlot slot : compKeyExpr) {
                // If the position of the pk columns in the query skips any part of the row key
                // then we have to handle in the next phase through a key filter.
                if (slot.getPosition() != pkColumns.get(pkPos + 1).getPosition()) {
                    break;
                }
                keyExpr = slot.getKeyPart();
                pkPos = slot.getPosition();
                List<KeyRange> ranges = keyExpr.getKeyRanges();

                KeyRange startRange = ranges.get(0);
                KeyRange stopRange = ranges.get(ranges.size() - 1);
                PDatum datum = keyExpr.getDatum();
                schema.addField(datum);

                List<KeyRange> filterHints = new ArrayList<KeyRange>();
                cnf.add(filterHints);

                // Add null byte separator to key parts if previous was variable length
                // Note that for a trailing var length a null byte separator should not be
                // added because it will conditionally be added depending on the operators
                // used.
                if (lastLowerVarLength) {
                    startKey.write(SEPARATOR_BYTE);
                }
                if (lastUpperVarLength) {
                    stopKey.write(SEPARATOR_BYTE);
                }

                /*
                 * Use SkipScanFilter under two circumstances:
                 * 1) If we have multiple ranges for a given key slot (use of IN)
                 * 2) If we have a range (i.e. not a single/point key) that is
                 *    not the last key slot
                 */
                useSkipScan |= ranges.size() > 1 | hasRangeKey;
                for (KeyRange range : ranges) {
                    hasRangeKey |= !range.isSingleKey();
                    filterHints.add(range);
                }

                // Concatenate each part of key together
                // If either condition is false, then loop exits below
                // TODO: write 1 here so null is filtered?
                if (!startRange.lowerUnbound()) {
                    // TODO: next byte if previous was an lower unbound range and blah
                    startKey.write(startRange.getLowerRange());
                    lastLowerInclusive = startRange.isLowerInclusive();
                    lastLowerVarLength = !datum.getDataType().isFixedWidth();
                    lowerPosCount++;
                }
                if (!stopRange.upperUnbound()) {
                    // TODO: next byte if previous was a range and blah
                    stopKey.write(stopRange.getUpperRange());
                    lastUpperInclusive = stopRange.isUpperInclusive();
                    lastUpperVarLength = !datum.getDataType().isFixedWidth();
                    upperPosCount++;
                }
                // Will be null in cases for which only part of the expression was factored out here
                // to set the start/end key. An example would be <column> LIKE 'foo%bar' where we can
                // set the start key to 'foo' but still need to match the regex at filter time.
                List<Expression> nodesToExtract = keyExpr.getExtractNodes();
                extractNodes.addAll(nodesToExtract);
                // Stop building start/stop key once we encounter a non single key range.
                // TODO: remove this soon after more testing on SkipScanFilter
                if (hasRangeKey) {
                    isFullyQualifiedKey = false;
                    // TODO: when stats are available, we will want to terminate this loop if we have
                    // a non single key range, depending on the cardinality of the column values.
                    break;
                }
            }

            // Tack on null byte if variable length, since otherwise we'd end up
            // getting everything that starts with the start key, not just that
            // is equal to the start key. Since LIKE is not variable length,
            // we wouldn't write this null byte.
            // Don't write a separator byte if we're at the last column since
            // we only put the separator between columns, not at the end.
            // TODO: create separate object for scan key formulation
            boolean isLastPKColumn = pkPos == table.getPKColumns().size() - 1;
            if (lastLowerVarLength && !lastLowerInclusive && !isLastPKColumn) {
                startKey.write(SEPARATOR_BYTE);
            }
            byte[] finalStartKey = KeyRange.UNBOUND_LOWER;
            if (startKey.size() > 0) {
                finalStartKey = startKey.toByteArray();
            }
            if (lastUpperVarLength && lastUpperInclusive && !isLastPKColumn) {
                stopKey.write(SEPARATOR_BYTE);
            }
            byte[] finalStopKey = KeyRange.UNBOUND_UPPER;
            if (stopKey.size() > 0) {
                finalStopKey = stopKey.toByteArray();
            }
            // TODO: PTable.getRowKeySchema()
            ScanKey scanKey =
                    new ScanKey(finalStartKey,
                            lastLowerInclusive,
                            lowerPosCount,
                            finalStopKey,
                            lastUpperInclusive,
                            upperPosCount,
                            table.getRowKeySchema(),
                            isFullyQualifiedKey && allPKColumnsUsed(table, pkPos+1));
            context.setScanKey(scanKey);

            if (useSkipScan) {
                context.setSkipScanFilter(new SkipScanFilter(cnf, table.getRowKeySchema()));
            }
            return whereClause.accept(new RemoveExtractedNodesVisitor(extractNodes));
        } finally {
            if (startKey != null) {
                try {
                    startKey.close();
                } catch (IOException e) {
                    throw new RuntimeException(e); // Impossible
                }
            }
            if (stopKey != null) {
                try {
                    stopKey.close();
                } catch (IOException e) {
                    throw new RuntimeException(e); // Impossible
                }
            }
        }
    }

    private static final boolean allPKColumnsUsed(PTable table, int nColUsed) {
        for (int i = nColUsed; i < table.getPKColumns().size(); i++) {
            PColumn column = table.getPKColumns().get(i);
            if (!column.isNullable()) {
                return false;
            }
        }
        return true;
    }

    private static class RemoveExtractedNodesVisitor extends TraverseNoExpressionVisitor<Expression> {
        private final Set<Expression> nodesToRemove;

        private RemoveExtractedNodesVisitor(Set<Expression> nodesToRemove) {
            this.nodesToRemove = nodesToRemove;
        }

        @Override
        public Expression defaultReturn(Expression node, List<Expression> e) {
            return nodesToRemove.contains(node) ? null : node;
        }

        @Override
        public Iterator<Expression> visitEnter(AndExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public Expression visitLeave(AndExpression node, List<Expression> l) {
            if (l.size() != node.getChildren().size()) {
                if (l.isEmpty()) {
                    // Don't return null here, because then our defaultReturn will kick in
                    return LiteralExpression.TRUE_EXPRESSION;
                }
                if (l.size() == 1) {
                    return l.get(0);
                }
                return new AndExpression(l);
            }
            return node;
        }

        // TODO: same visitEnter/visitLeave for OrExpression once we optimize it
    }

    public static class KeyExpressionVisitor extends TraverseAllExpressionVisitor<KeyExpressionVisitor.KeyParts> {
        private static final KeyPart DEGENERATE_KEY_EXPRESSION = new KeyPart(null, 0, Collections.<Expression>emptyList(), KeyRange.EMPTY_RANGE, null);

        private static KeyPart newKeyExpression(PDatum backingDatum, int position, List<Expression> nodes, boolean isEquality, KeyRange keyRange, PDatum datum) {
            if (keyRange == KeyRange.EMPTY_RANGE) {
                return DEGENERATE_KEY_EXPRESSION;
            } else {
                return new KeyPart(backingDatum, position, nodes, keyRange, datum);
            }
        }

        private static KeyPart newKeyExpression(PDatum backingDatum, int position, List<Expression> nodes, List<KeyRange> keyRanges, PDatum datum) {
            assert keyRanges != null;
            assert !keyRanges.isEmpty();
            if (keyRanges.get(0) == KeyRange.EMPTY_RANGE) {
                return DEGENERATE_KEY_EXPRESSION;
            } else {
                return new KeyPart(backingDatum, position, nodes, keyRanges, datum);
            }
        }

        private static KeyPart newScalarFunctionKeyPart(KeyPart part, ScalarFunction node) {
            if (part.getKeyRanges() == null || part.getKeyRanges().size() == 1 && part.getKeyRanges().get(0) == KeyRange.EMPTY_RANGE) {
                return DEGENERATE_KEY_EXPRESSION;
            }
            return node.newKeyPart(part);
        }

        private static KeyParts andKeyExpression(int nColumns, List<KeyParts> childKeyExprs) {
            KeyPart[] expressions = new KeyPart[nColumns];
            for (KeyParts childKeyExpr : childKeyExprs) {
                for (KeySlot slot : childKeyExpr) {
                    int position = slot.getPosition();
                    KeyPart existing = expressions[position];
                    if (existing == null) {
                        expressions[position] = slot.getKeyPart();
                    } else {
                        expressions[position] = existing.intersect(slot.getKeyPart());
                    }
                    if (expressions[position] == DEGENERATE_KEY_EXPRESSION) {
                        return DEGENERATE_KEY_EXPRESSION;
                    }
                }
            }

            return new MultiKeyPart(expressions);
        }

        private final StatementContext context;

        public KeyExpressionVisitor(StatementContext context) {
            this.context = context;
        }

        @Override
        public KeyParts defaultReturn(Expression node, List<KeyParts> l) {
            // Passes the CompositeKeyExpression up the tree
            return l.size() == 1 ? l.get(0) : null;
        }

        @Override
        public KeyParts visitLeave(AndExpression node, List<KeyParts> l) {
            List<TableRef> tables = context.getResolver().getTables();
            assert tables.size() == 1;
            int nColumns = tables.get(0).getTable().getPKColumns().size();
            KeyParts keyExpr = andKeyExpression(nColumns, l);
            return keyExpr;
        }

        @Override
        public KeyParts visit(RowKeyColumnExpression node) {
            return newKeyExpression(node, node.getPosition(), Collections.<Expression>singletonList(node), false, KeyRange.EVERYTHING_RANGE, node);
        }

        // TODO: get rid of datum and backing datum and just use the extracted node
        // If extracted node becomes empty, then nothing to contribute for that branch
        // Ok to pass up original backing datum all the way up to the point were exract
        // node means something - don't need to pass function expression up, I don't think
        @Override
        public Iterator<Expression> visitEnter(ComparisonExpression node) {
            Expression rhs = node.getChildren().get(1);
            if (! (rhs instanceof LiteralExpression)  || node.getFilterOp() == CompareOp.NOT_EQUAL) {
                return Iterators.emptyIterator();
            }
            return Iterators.singletonIterator(node.getChildren().get(0));
        }

        @Override
        public KeyParts visitLeave(ComparisonExpression node, List<KeyParts> childKeyExprs) {
            // Delay adding to extractedNodes, until we're done traversing,
            // since we can't yet tell whether or not the PK column references
            // are continguous
            if (childKeyExprs.isEmpty()) {
                return null;
            }
            Expression datum = node.getChildren().get(0);
            // If we have a keyLength, then we need to wrap the column with a delegate
            // that reflects the subsetting done by the function invocation. Else if
            // keyLength is null, then the underlying function preserves order and
            // does not subsetting and can then be ignored.
            byte[] key = ((LiteralExpression)node.getChildren().get(1)).getBytes();
            // If the expression is an equality expression against a fixed length column
            // and the key length doesn't match the column length, the expression can
            // never be true.
            // An zero length byte literal is null which can never be compared against as true
            // TODO: how to detect if column is tenant column?
            // Mark on RowKeyExpression? Difficult b/c we don't have a PColumn
            // Lookup name through ColumnResolver.getTable via index?
            // Use some other mechanism like setting a context variable on connect?
            // Might be best b/c it allows clients to control more
            if (node.getFilterOp() == CompareOp.EQUAL
                && datum.getDataType().isFixedWidth()
                && key.length != datum.getByteSize()) {
                return DEGENERATE_KEY_EXPRESSION;
            }
            // TODO: we need SUBSTR to be handled like LIKE and use CompareOp.NO_OP,
            // so that it's not handled as a single key, but as a range. The building
            // of the function should indicate an override for the operator?
            // What's the best way to do that?
            KeyPart childKeyPart = childKeyExprs.get(0).iterator().next().getKeyPart();
            KeyRange keyRange = childKeyPart.getKeyRange(node.getFilterOp(), key);
            List<Expression> extractNodes = childKeyPart.getExtractNodes().isEmpty() ? Collections.<Expression>emptyList() : Collections.<Expression>singletonList(node);
            return newKeyExpression(
                    childKeyPart.getDatum(),
                    childKeyPart.getPosition(),
                    extractNodes,
                    node.getFilterOp() == CompareOp.EQUAL,
                    keyRange,
                    datum);
        }

        // TODO: consider supporting expression substitution in the PK for pre-joined tables
        // You'd need to register the expression for a given PK and substitute with a column
        // reference for this during ExpressionBuilder.
        @Override
        public Iterator<Expression> visitEnter(ScalarFunction node) {
            if (node.getKeyFormationDirective() == KeyFormationDirective.UNTRAVERSABLE) {
                return Iterators.emptyIterator();
            }
            return Iterators.singletonIterator(node.getChildren().get(node.getKeyFormationTraversalIndex()));
        }

        @Override
        public KeyParts visitLeave(ScalarFunction node, List<KeyParts> childKeyExprs) {
            if (childKeyExprs.isEmpty()) {
                return null;
            }
            return newScalarFunctionKeyPart(childKeyExprs.get(0).iterator().next().getKeyPart(), node);
        }

        @Override
        public Iterator<Expression> visitEnter(LikeExpression node) {
            // TODO: can we optimize something that starts with '_' like this: foo LIKE '_a%'
            if (! (node.getChildren().get(1) instanceof LiteralExpression) || node.startsWithWildcard()) {
                return Iterators.emptyIterator();
            }

            return super.visitEnter(node);
        }

        @Override
        public KeyParts visitLeave(LikeExpression node, List<KeyParts> childKeyExprs) {
            if (childKeyExprs.isEmpty()) {
                return null;
            }
            // for SUBSTR(<column>,1,3) LIKE 'foo%'
            PDatum backingDatum = node.getChildren().get(0);
            final String startsWith = node.getLiteralPrefix();
            byte[] key = PDataType.CHAR.toBytes(startsWith);
            // If the expression is an equality expression against a fixed length column
            // and the key length doesn't match the column length, the expression can
            // never be true.
            // An zero length byte literal is null which can never be compared against as true
            if (backingDatum.getDataType().isFixedWidth()
                && key.length > backingDatum.getByteSize()) {
                return DEGENERATE_KEY_EXPRESSION;
            }
            PDatum datum = new DelegateDatum(backingDatum) {
                @Override
                public Integer getByteSize() {
                    return startsWith.length();
                }
                @Override
                public PDataType getDataType() {
                    return PDataType.CHAR;
                }
            };
            KeyPart childKeyPart = childKeyExprs.get(0).iterator().next().getKeyPart();
            KeyRange keyRange = childKeyPart.getKeyRange(CompareOp.NO_OP, key);
            // Only extract LIKE expression if pattern ends with a wildcard and everything else was extracted
            List<Expression> extractNodes = node.endsWithOnlyWildcard() ? Collections.<Expression>singletonList(node) : Collections.<Expression>emptyList();
            return newKeyExpression(backingDatum, childKeyPart.getPosition(), extractNodes, false, keyRange, datum);
        }

        @Override
        public Iterator<Expression> visitEnter(CaseExpression node) {
            // TODO: optimize for simple case statement with all constants
            return Iterators.emptyIterator();
        }

        @Override
        public Iterator<Expression> visitEnter(InListExpression node) {
            return Iterators.singletonIterator(node.getChildren().get(0));
        }

        // TODO: optimize same as OR by having set of key ranges
        @Override
        public KeyParts visitLeave(InListExpression node, List<KeyParts> childKeyExprs) {
            if (childKeyExprs.isEmpty()) {
                return null;
            }

            List<byte[]> keys = node.getKeys();
            List<KeyRange> ranges = KeyRange.of(keys);
            KeyPart colKeyExpr = childKeyExprs.get(0).iterator().next().getKeyPart();
            if (ranges.size() > 0) {
                // IN cannot contain unbound ranges
                assert(!ranges.get(0).lowerUnbound() && !ranges.get(ranges.size() - 1).upperUnbound());
                ImmutableBytesWritable minKey = node.getMinKey();
                ImmutableBytesWritable maxKey = node.getMaxKey();
                // TODO: make key range backed by ImmutableBytesWritable to prevent copy?
                KeyRange keyRange = KeyRange.getKeyRange(minKey.copyBytes(), true, maxKey.copyBytes(), true);
                return newKeyExpression(colKeyExpr.getDatum(), colKeyExpr.getPosition(), Collections.<Expression>emptyList(), false, keyRange, node.getChildren().get(0));
            }
            return newKeyExpression(colKeyExpr.getDatum(), colKeyExpr.getPosition(), Collections.<Expression>singletonList(node), ranges, node.getChildren().get(0));
        }

        @Override
        public Iterator<Expression> visitEnter(IsNullExpression node) {
            // TODO: use (0,*) for isNegate?
            return node.isNegate() ? Iterators.<Expression>emptyIterator() : Iterators.singletonIterator(node.getChildren().get(0));
        }

        @Override
        public KeyParts visitLeave(IsNullExpression node, List<KeyParts> childKeyExprs) {
            if (childKeyExprs.isEmpty()) {
                return null;
            }
            KeyPart colKeyExpr = childKeyExprs.get(0).iterator().next().getKeyPart();
            // TODO: make key range backed by ImmutableBytesWritable to prevent copy?
            KeyRange keyRange = KeyRange.getKeyRange(ByteUtil.EMPTY_BYTE_ARRAY, true, ByteUtil.EMPTY_BYTE_ARRAY, true);
            return newKeyExpression(colKeyExpr.getDatum(), colKeyExpr.getPosition(), Collections.<Expression>singletonList(node), true, keyRange, node.getChildren().get(0));
        }

        // TODO: rethink default: probably better if we don't automatically walk through constructs

        @Override
        public Iterator<Expression> visitEnter(OrExpression node) {
            // TODO: optimize for cases where LHS column is the same for all
            return Iterators.emptyIterator();
        }

        @Override
        public Iterator<Expression> visitEnter(StringConcatExpression node) {
            // TODO: we could optimize by substring-ing the RHS if
            // the first child is a KeyPart and the rest of the children
            // are constants. For example: WHERE foo||'bar'='1234bar' we'd
            // need to replace the rhs with SUBSTR(rhs,-<size-of-constant-strings>)
            return Iterators.emptyIterator();
        }

        /*
         * These prevents our optimizer from trying to form a rowkey for a column in
         * an arithmetic expression. We could possibly do more later, but for now folks
         * should write their expressions with constants on the LHS. For example:
         *    WHERE a + 1 < 5
         * would cause the rowkey to not be able to formed using column a, while
         *    WHERE a < 4
         * would be able to be formed using column a.
         **/
        @Override
        public Iterator<Expression> visitEnter(AddExpression node) {
            // TODO: optimize by moving constants to LHS
            return Iterators.emptyIterator();
        }

        @Override
        public Iterator<Expression> visitEnter(SubtractExpression node) {
            // TODO: optimize by moving constants to LHS
            return Iterators.emptyIterator();
        }

        @Override
        public Iterator<Expression> visitEnter(MultiplyExpression node) {
            // TODO: optimize by moving constants to LHS
            return Iterators.emptyIterator();
        }

        @Override
        public Iterator<Expression> visitEnter(DivideExpression node) {
            // TODO: optimize by moving constants to LHS
            return Iterators.emptyIterator();
        }

        @Override
        public Iterator<Expression> visitEnter(NotExpression node) {
            return Iterators.emptyIterator();
        }

        private static interface KeyParts extends Iterable<KeySlot> {
            @Override public Iterator<KeySlot> iterator();
        }

        private static final class KeySlot {
            private int position;
            private KeyPart keyPart;

            private KeySlot() {
            }

            private KeySlot(int position, KeyPart keyPart) {
                this.position = position;
                this.keyPart = keyPart;
            }

            public void setPosition(int position) {
                this.position = position;
            }

            public int getPosition() {
                return position;
            }

            public void setKeyPart(KeyPart keyPart) {
                this.keyPart = keyPart;
            }

            public KeyPart getKeyPart() {
                return keyPart;
            }
        }

        private static class MultiKeyPart implements KeyParts {
            private final KeyPart[] expressions;

            private MultiKeyPart(KeyPart[] expressions) {
                this.expressions = expressions;
            }

            @Override
            public Iterator<KeySlot> iterator() {
                final KeySlot keySlot = new KeySlot();
                keySlot.setPosition(-1);

                return new Iterator<KeySlot> () {
                    @Override
                    public boolean hasNext() {
                        while (keySlot.getPosition() + 1 < expressions.length) {
                            if (expressions[keySlot.getPosition() + 1] != null) {
                                break;
                            }
                            keySlot.setPosition(keySlot.getPosition() + 1);
                        }
                        return keySlot.getPosition() + 1 < expressions.length;
                    }

                    @Override
                    public KeySlot next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        keySlot.setPosition(keySlot.getPosition() + 1);
                        keySlot.setKeyPart(expressions[keySlot.getPosition()]);
                        return keySlot;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                };
            }
        }

        public static class KeyPart implements KeyParts {
            /**
             * Fill both upper and lower range of keyRange to keyLength bytes.
             * If the upper bound is inclusive, it must be filled such that an
             * intersection with a longer key would still match if the shorter
             * length matches.  For example: (*,00C] intersected with [00Caaa,00Caaa]
             * should still return [00Caaa,00Caaa] since the 00C matches and is
             * inclusive.
             * @param keyRange
             * @param keyLength
             * @return
             */
            protected static KeyRange fillKey(KeyRange keyRange, int keyLength) {
                byte[] lowerRange = keyRange.getLowerRange();
                byte[] newLowerRange = lowerRange;
                if (!keyRange.lowerUnbound()) {
                    // If lower range is inclusive, fill with 0x00 since conceptually these bytes are included in the range
                    newLowerRange = ByteUtil.fillKey(lowerRange, (byte)(keyRange.isLowerInclusive() ? 0 : -1), keyLength);
                }
                byte[] upperRange = keyRange.getUpperRange();
                byte[] newUpperRange = upperRange;
                if (!keyRange.upperUnbound()) {
                    // If upper range is inclusive, fill with 0xFF since conceptually these bytes are included in the range
                    newUpperRange = ByteUtil.fillKey(upperRange, (byte)(keyRange.isUpperInclusive() ? -1 : 0), keyLength);
                }
                if (newLowerRange != lowerRange || newUpperRange != upperRange) {
                    return KeyRange.getKeyRange(newLowerRange, keyRange.isLowerInclusive(), newUpperRange, keyRange.isUpperInclusive());
                }
                return keyRange;
            }

            public KeyRange getKeyRange(CompareOp op, byte[] key) {
                KeyRange range;
                switch (op) {
                case EQUAL:
                    range = KeyRange.getKeyRange(key, true, key, true);
                    break;
                case NO_OP: // using for LIKE: make it so that upper != lower to it's not treated like a single key
                    range = KeyRange.getKeyRange(key, true, ByteUtil.nextKey(key), false);
                    break;
                case GREATER:
                    range = KeyRange.getKeyRange(key, false, KeyRange.UNBOUND_UPPER, false);
                    break;
                case GREATER_OR_EQUAL:
                    return KeyRange.getKeyRange(key, true, KeyRange.UNBOUND_UPPER, false);
                case LESS:
                    range = KeyRange.getKeyRange(KeyRange.UNBOUND_LOWER, false, key, false);
                    break;
                case LESS_OR_EQUAL:
                    range = KeyRange.getKeyRange(KeyRange.UNBOUND_LOWER, false, key, true);
                    break;
                default:
                    return null;
                }
                Integer length = this.getBackingDatum().getDataType().isFixedWidth() ? this.getBackingDatum().getByteSize() : null;
                return length == null ? range : fillKey(range, length);
            }

            private final PDatum datum;
            private final PDatum backingDatum;
            private final KeySlot keySlot;
            // sorted non-overlapping key ranges.  may be empty, but won't be null or contain nulls
            private final List<KeyRange> keyRanges;
            private final List<Expression> nodes;

            protected KeyPart(PDatum backingDatum, int position, List<Expression> nodes, KeyRange keyRange, PDatum datum) {
              this(backingDatum, position, nodes, Collections.singletonList(keyRange), datum);
            }

            public KeyPart(PDatum backingDatum, int position, List<Expression> nodes, List<KeyRange> keyRanges, PDatum datum) {
              this.datum = datum;
              this.keySlot = new KeySlot(position, this);
              this.nodes = nodes;
              this.backingDatum = backingDatum;
              this.keyRanges = KeyRange.coalesce(keyRanges);
          }

            public final KeyPart intersect(KeyPart k2) {
                KeyPart k1 = this;
                if (k1 == DEGENERATE_KEY_EXPRESSION || k2 == DEGENERATE_KEY_EXPRESSION) {
                    return DEGENERATE_KEY_EXPRESSION;
                }
                if (k1.keySlot.getPosition() != k2.keySlot.getPosition()) {
                    throw new IllegalArgumentException("Position must be equal for intersect");
                }
                Preconditions.checkArgument(!k1.keyRanges.isEmpty());
                Preconditions.checkArgument(!k2.keyRanges.isEmpty());

                return newKeyExpression(
                        k1.getBackingDatum(),
                        k1.keySlot.getPosition(),
                        SchemaUtil.concat(k1.getExtractNodes(), k2.getExtractNodes()),
                        KeyRange.intersect(k1.getKeyRanges(), k2.getKeyRanges()),
                        k1.getDatum());
            }

            public List<KeyRange> getKeyRanges() {
                return keyRanges;
            }

            public PDatum getDatum() {
                return datum;
            }

            public List<Expression> getExtractNodes() {
                return nodes;
            }

            public PDatum getBackingDatum() {
                return backingDatum;
            }

            public int getPosition() {
                return keySlot.getPosition();
            }

            @Override
            public Iterator<KeySlot> iterator() {
                return Iterators.singletonIterator(keySlot);
            }
        }
        public static class StartsWithKeyPart extends KeyPart {
            
            public StartsWithKeyPart(PDatum backingDatum, int position, List<Expression> nodes, List<KeyRange> keyRanges,
                    PDatum datum) {
                super(backingDatum, position, nodes, keyRanges, datum);
            }

            @Override
            public KeyRange getKeyRange(CompareOp op, byte[] key) {
                KeyRange range;
                switch (op) {
                case EQUAL:
                    range = KeyRange.getKeyRange(key, true, ByteUtil.nextKey(key), false);
                    break;
                case LESS_OR_EQUAL:
                    range = KeyRange.getKeyRange(KeyRange.UNBOUND_LOWER, false, ByteUtil.nextKey(key), false);
                    break;
                default:
                    range = super.getKeyRange(op, key);
                    break;
                }
                Integer length = this.getBackingDatum().getDataType().isFixedWidth() ? this.getBackingDatum().getMaxLength() : null;
                return length == null ? range : fillKey(range, length);
            }

        }
    }
}
