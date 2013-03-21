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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.expression.function.ScalarFunction;
import com.salesforce.phoenix.expression.visitor.TraverseAllExpressionVisitor;
import com.salesforce.phoenix.expression.visitor.TraverseNoExpressionVisitor;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.*;
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
        // TODO: Single table for now
        PTable table = context.getResolver().getTables().get(0).getTable();
        KeyExpressionVisitor visitor = new KeyExpressionVisitor(context, table);
        KeyExpressionVisitor.KeySlots keySlots = whereClause.accept(visitor);

        if (keySlots == null) {
            context.setScanKey(ScanKey.EVERYTHING_SCAN_KEY);
            return whereClause;
        }
        // If a parameter is bound to null (as will be the case for calculating ResultSetMetaData and
        // ParameterMetaData), this will be the case. It can also happen for an equality comparison
        // for unequal lengths.
        if (keySlots == KeyExpressionVisitor.DEGENERATE_KEY_PARTS) {
            context.setScanKey(ScanKey.DEGENERATE_SCAN_KEY);
            return null;
        }

        RowKeySchema schema = table.getRowKeySchema();
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
            int lowerPosCount = 0;
            int upperPosCount = 0;
            int pkPos = -1;
            boolean isFullyQualifiedKey = true;
            List<List<KeyRange>> cnf = new ArrayList<List<KeyRange>>();
            boolean useSkipScan = false;
            boolean hasRangeKey = false;
            // Concat byte arrays of literals to form scan start key
            for (KeyExpressionVisitor.KeySlot slot : keySlots) {
                // If the position of the pk columns in the query skips any part of the row k
                // then we have to handle in the next phase through a key filter.
                // If the slot is null this means we have no entry for this pk position.
                if (slot == null || slot.getPKPosition() != pkPos + 1) {
                    break;
                }
                KeyPart keyPart = slot.getKeyPart();
                pkPos = slot.getPKPosition();
                List<KeyRange> ranges = slot.getKeyRanges();

                KeyRange startRange = ranges.get(0);
                KeyRange stopRange = ranges.get(ranges.size() - 1);

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

                // Reset these - we'll only want to append a null byte
                // if we're not unbound. Otherwise the key won't be
                // correct for IS NOT NULL cases.
                lastLowerVarLength = false;
                lastUpperVarLength = false;
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
                    lastLowerVarLength = !schema.getField(pkPos).getType().isFixedWidth();
                    lowerPosCount++;
                }
                if (!stopRange.upperUnbound()) {
                    // TODO: next byte if previous was a range and blah
                    stopKey.write(stopRange.getUpperRange());
                    lastUpperInclusive = stopRange.isUpperInclusive();
                    lastUpperVarLength = !schema.getField(pkPos).getType().isFixedWidth();
                    upperPosCount++;
                }
                // Will be null in cases for which only part of the expression was factored out here
                // to set the start/end key. An example would be <column> LIKE 'foo%bar' where we can
                // set the start key to 'foo' but still need to match the regex at filter time.
                List<Expression> nodesToExtract = keyPart.getExtractNodes();
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
                            schema,
                            isFullyQualifiedKey && allPKColumnsUsed(table, pkPos+1));
            context.setScanKey(scanKey);

            if (useSkipScan) {
                context.setSkipScanFilter(new SkipScanFilter(cnf, schema));
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

    public static class KeyExpressionVisitor extends TraverseAllExpressionVisitor<KeyExpressionVisitor.KeySlots> {
        private static final List<KeyRange> EVERYTHING_RANGES = Collections.<KeyRange>singletonList(KeyRange.EVERYTHING_RANGE);
        private static final KeySlots DEGENERATE_KEY_PARTS = new KeySlots() {
            @Override
            public Iterator<KeySlot> iterator() {
                return Iterators.emptyIterator();
            }
        };

        private static boolean isDegenerate(List<KeyRange> keyRanges) {
            return keyRanges == null || keyRanges.size() == 1 && keyRanges.get(0) == KeyRange.EMPTY_RANGE;
        }
        
        private static KeySlots newKeyParts(KeySlot slot, Expression extractNode, KeyRange keyRange) {
            return newKeyParts(slot, extractNode, Collections.<KeyRange>singletonList(keyRange));
        }

        private static KeySlots newKeyParts(KeySlot slot, Expression extractNode, List<KeyRange> keyRanges) {
            if (isDegenerate(keyRanges)) {
                return DEGENERATE_KEY_PARTS;
            }
            
            List<Expression> extractNodes = extractNode == null || slot.getKeyPart().getExtractNodes().isEmpty()
                  ? Collections.<Expression>emptyList()
                  : Collections.<Expression>singletonList(extractNode);
            return new SingleKeySlot(new BaseKeyPart(slot.getKeyPart().getColumn(), extractNodes), slot.getPKPosition(), keyRanges);
        }

        private static KeySlots newScalarFunctionKeyPart(KeySlot slot, ScalarFunction node) {
            if (isDegenerate(slot.getKeyRanges())) {
                return DEGENERATE_KEY_PARTS;
            }
            KeyPart part = node.newKeyPart(slot.getKeyPart());
            if (part == null) {
                return null;
            }
            
            return new SingleKeySlot(part, slot.getPKPosition(), slot.getKeyRanges());
        }

        private static KeySlots andKeyExpression(int nColumns, List<KeySlots> childSlots) {
            KeySlot[] newChildSlots = new KeySlot[nColumns];
            for (KeySlots childSlot : childSlots) {
                if (childSlot == DEGENERATE_KEY_PARTS) {
                    return DEGENERATE_KEY_PARTS;
                }
                for (KeySlot slot : childSlot) {
                    // We have a nested AND with nothing for this slot, so continue
                    if (slot == null) {
                        continue;
                    }
                    int position = slot.getPKPosition();
                    KeySlot existing = newChildSlots[position];
                    if (existing == null) {
                        newChildSlots[position] = slot;
                    } else {
                        newChildSlots[position] = existing.intersect(slot);
                        if (newChildSlots[position] == null) {
                            return DEGENERATE_KEY_PARTS;
                        }
                    }
                }
            }

            return new MultiKeySlot(Arrays.asList(newChildSlots));
        }

        private final StatementContext context;
        private final PTable table;

        public KeyExpressionVisitor(StatementContext context, PTable table) {
            this.context = context;
            this.table = table;
        }

        @Override
        public KeySlots defaultReturn(Expression node, List<KeySlots> l) {
            // Passes the CompositeKeyExpression up the tree
            return l.size() == 1 ? l.get(0) : null;
        }

        @Override
        public KeySlots visitLeave(AndExpression node, List<KeySlots> l) {
            List<TableRef> tables = context.getResolver().getTables();
            assert tables.size() == 1;
            int nColumns = table.getPKColumns().size();
            KeySlots keyExpr = andKeyExpression(nColumns, l);
            return keyExpr;
        }

        @Override
        public KeySlots visit(RowKeyColumnExpression node) {
            PColumn column = table.getPKColumns().get(node.getPosition());
            return new SingleKeySlot(new BaseKeyPart(column, Collections.<Expression>singletonList(node)), node.getPosition(), EVERYTHING_RANGES);
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
        public KeySlots visitLeave(ComparisonExpression node, List<KeySlots> childParts) {
            // Delay adding to extractedNodes, until we're done traversing,
            // since we can't yet tell whether or not the PK column references
            // are contiguous
            if (childParts.isEmpty()) {
                return null;
            }
            // If we have a keyLength, then we need to wrap the column with a delegate
            // that reflects the subsetting done by the function invocation. Else if
            // keyLength is null, then the underlying function preserves order and
            // does not subsetting and can then be ignored.
            byte[] key = ((LiteralExpression)node.getChildren().get(1)).getBytes();
            // If the expression is an equality expression against a fixed length column
            // and the key length doesn't match the column length, the expression can
            // never be true.
            Integer fixedLength = node.getChildren().get(0).getByteSize();
            if (node.getFilterOp() == CompareOp.EQUAL && fixedLength != null && key.length != fixedLength) {
                return DEGENERATE_KEY_PARTS;
            }
            KeySlot childSlot = childParts.get(0).iterator().next();
            KeyRange keyRange = childSlot.getKeyPart().getKeyRange(node.getFilterOp(), key);
            return newKeyParts(childSlot, node, keyRange);
        }

        // TODO: consider supporting expression substitution in the PK for pre-joined tables
        // You'd need to register the expression for a given PK and substitute with a column
        // reference for this during ExpressionBuilder.
        @Override
        public Iterator<Expression> visitEnter(ScalarFunction node) {
            int index = node.getKeyFormationTraversalIndex();
            if (index < 0) {
                return Iterators.emptyIterator();
            }
            return Iterators.singletonIterator(node.getChildren().get(index));
        }

        @Override
        public KeySlots visitLeave(ScalarFunction node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }
            return newScalarFunctionKeyPart(childParts.get(0).iterator().next(), node);
        }

        @Override
        public Iterator<Expression> visitEnter(LikeExpression node) {
            // TODO: can we optimize something that starts with '_' like this: foo LIKE '_a%' ?
            if (! (node.getChildren().get(1) instanceof LiteralExpression) || node.startsWithWildcard()) {
                return Iterators.emptyIterator();
            }

            return super.visitEnter(node);
        }

        @Override
        public KeySlots visitLeave(LikeExpression node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }
            // for SUBSTR(<column>,1,3) LIKE 'foo%'
            KeySlot childSlot = childParts.get(0).iterator().next();
            final String startsWith = node.getLiteralPrefix();
            byte[] key = PDataType.CHAR.toBytes(startsWith);
            // If the expression is an equality expression against a fixed length column
            // and the key length doesn't match the column length, the expression can
            // never be true.
            // An zero length byte literal is null which can never be compared against as true
            Integer childNodeFixedLength = node.getChildren().get(0).getByteSize();
            if (childNodeFixedLength != null && key.length > childNodeFixedLength) {
                return DEGENERATE_KEY_PARTS;
            }
            // TODO: is there a case where we'd need to go through the childPart to calculate the key range?
            Integer columnFixedLength = childSlot.getKeyPart().getColumn().getByteSize();
            KeyRange keyRange = KeyRange.getKeyRange(key, true, ByteUtil.nextKey(key), false);
            if (columnFixedLength != null) {
                keyRange = keyRange.fill(columnFixedLength);
            }
            // Only extract LIKE expression if pattern ends with a wildcard and everything else was extracted
            return newKeyParts(childSlot, node.endsWithOnlyWildcard() ? node : null, keyRange);
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
        public KeySlots visitLeave(InListExpression node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }

            List<byte[]> keys = node.getKeys();
            List<KeyRange> ranges = Lists.newArrayListWithExpectedSize(keys.size());
            KeySlot childSlot = childParts.get(0).iterator().next();
            KeyPart childPart = childSlot.getKeyPart();
            // Handles cases like WHERE substr(foo,1,3) IN ('aaa','bbb')
            for (byte[] key : keys) {
                ranges.add(childPart.getKeyRange(CompareOp.EQUAL, key));
            }
            return newKeyParts(childSlot, node, ranges);
        }

        @Override
        public Iterator<Expression> visitEnter(IsNullExpression node) {
            return Iterators.singletonIterator(node.getChildren().get(0));
        }

        @Override
        public KeySlots visitLeave(IsNullExpression node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }
            KeySlot childSlot = childParts.get(0).iterator().next();
            // TODO: go through childPart to make this range?
            KeyRange keyRange = node.isNegate() 
                    ? KeyRange.getKeyRange(ByteUtil.EMPTY_BYTE_ARRAY, false, KeyRange.UNBOUND_UPPER, true)
                    : KeyRange.getKeyRange(ByteUtil.EMPTY_BYTE_ARRAY, true, ByteUtil.EMPTY_BYTE_ARRAY, true);
            return newKeyParts(childSlot, node, keyRange);
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

        private static interface KeySlots extends Iterable<KeySlot> {
            @Override public Iterator<KeySlot> iterator();
        }

        private static final class KeySlot {
            private final int pkPosition;
            private final KeyPart keyPart;
            private final List<KeyRange> keyRanges;

            private KeySlot(KeyPart keyPart, int pkPosition, List<KeyRange> keyRanges) {
                this.pkPosition = pkPosition;
                this.keyPart = keyPart;
                this.keyRanges = keyRanges;
            }

            public KeyPart getKeyPart() {
                return keyPart;
            }

            public int getPKPosition() {
                return pkPosition;
            }

            public List<KeyRange> getKeyRanges() {
                return keyRanges;
            }

            public final KeySlot intersect(KeySlot that) {
                if (this.getPKPosition() != that.getPKPosition()) {
                    throw new IllegalArgumentException("Position must be equal for intersect");
                }
                Preconditions.checkArgument(!this.keyRanges.isEmpty());
                Preconditions.checkArgument(!that.keyRanges.isEmpty());

                List<KeyRange> keyRanges = KeyRange.intersect(this.getKeyRanges(), that.getKeyRanges());
                if (isDegenerate(keyRanges)) {
                    return null;
                }
                return new KeySlot(
                        new BaseKeyPart(this.getKeyPart().getColumn(),
                                    SchemaUtil.concat(this.getKeyPart().getExtractNodes(),
                                                      that.getKeyPart().getExtractNodes())),
                        this.getPKPosition(),
                        keyRanges);
            }
        }

        private static class MultiKeySlot implements KeySlots {
            private final List<KeySlot> childSlots;

            private MultiKeySlot(List<KeySlot> childSlots) {
                this.childSlots = childSlots;
            }

            @Override
            public Iterator<KeySlot> iterator() {
                return childSlots.iterator();
            }
        }

        private static class SingleKeySlot implements KeySlots {
            private final KeySlot slot;
            
            private SingleKeySlot(KeyPart part, int pkPosition, List<KeyRange> ranges) {
                this.slot = new KeySlot(part, pkPosition, ranges);
            }
            
            @Override
            public Iterator<KeySlot> iterator() {
                return Iterators.<KeySlot>singletonIterator(slot);
            }
            
        }
        
        private static class BaseKeyPart implements KeyPart {
            @Override
            public KeyRange getKeyRange(CompareOp op, byte[] key) {
                KeyRange range;
                switch (op) {
                case EQUAL:
                    range = KeyRange.getKeyRange(key, true, key, true);
                    break;
                case GREATER:
                    range = KeyRange.getKeyRange(key, false, KeyRange.UNBOUND_UPPER, false);
                    break;
                case GREATER_OR_EQUAL:
                    range = KeyRange.getKeyRange(key, true, KeyRange.UNBOUND_UPPER, false);
                    break;
                case LESS:
                    range = KeyRange.getKeyRange(KeyRange.UNBOUND_LOWER, false, key, false);
                    break;
                case LESS_OR_EQUAL:
                    range = KeyRange.getKeyRange(KeyRange.UNBOUND_LOWER, false, key, true);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operator " + op);
                }
                Integer length = getColumn().getByteSize();
                return length == null ? range : range.fill(length);
            }

            private final PColumn column;
            // sorted non-overlapping key ranges.  may be empty, but won't be null or contain nulls
            private final List<Expression> nodes;

            private BaseKeyPart(PColumn column, List<Expression> nodes) {
                this.column = column;
                this.nodes = nodes;
            }

            @Override
            public List<Expression> getExtractNodes() {
                return nodes;
            }

            @Override
            public PColumn getColumn() {
                return column;
            }
        }
    }
}
