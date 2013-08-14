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

import java.util.*;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.expression.function.ScalarFunction;
import com.salesforce.phoenix.expression.visitor.TraverseNoExpressionVisitor;
import com.salesforce.phoenix.parse.FilterableStatement;
import com.salesforce.phoenix.parse.HintNode.Hint;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.QueryConstants;
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
    private static final List<KeyRange> SALT_PLACEHOLDER = Collections.singletonList(PDataType.CHAR.getKeyRange(QueryConstants.SEPARATOR_BYTE_ARRAY));
    private WhereOptimizer() {
    }

    /**
     * Pushes row key expressions from the where clause into the start/stop key of the scan.
     * @param context the shared context during query compilation
     * @param statement TODO
     * @param whereClause the where clause expression
     * @return the new where clause with the key expressions removed
     */
    public static Expression pushKeyExpressionsToScan(StatementContext context, FilterableStatement statement, Expression whereClause) {
        return pushKeyExpressionsToScan(context, statement, whereClause, null);
    }

    // For testing so that the extractedNodes can be verified
    public static Expression pushKeyExpressionsToScan(StatementContext context, FilterableStatement statement,
            Expression whereClause, Set<Expression> extractNodes) {
        boolean forcedSkipScanFilter = statement.getHint().hasHint(Hint.SKIP_SCAN);
        if (whereClause == null) {
            context.setScanRanges(ScanRanges.EVERYTHING);
            return whereClause;
        }
        if (whereClause == LiteralExpression.FALSE_EXPRESSION) {
            context.setScanRanges(ScanRanges.NOTHING);
            return null;
        }
        // TODO: Single table for now
        PTable table = context.getResolver().getTables().get(0).getTable();
        KeyExpressionVisitor visitor = new KeyExpressionVisitor(table);
        // TODO:: When we only have one where clause, the keySlots returns as a single slot object,
        // instead of an array of slots for the corresponding column. Change the behavior so it
        // becomes consistent.
        KeyExpressionVisitor.KeySlots keySlots = whereClause.accept(visitor);

        if (keySlots == null) {
            context.setScanRanges(ScanRanges.EVERYTHING);
            return whereClause;
        }
        // If a parameter is bound to null (as will be the case for calculating ResultSetMetaData and
        // ParameterMetaData), this will be the case. It can also happen for an equality comparison
        // for unequal lengths.
        if (keySlots == KeyExpressionVisitor.DEGENERATE_KEY_PARTS) {
            context.setScanRanges(ScanRanges.NOTHING);
            return null;
        }

        if (extractNodes == null) {
            extractNodes = new HashSet<Expression>(table.getPKColumns().size());
        }

        int pkPos = table.getBucketNum() == null ? -1 : 0;
        LinkedList<List<KeyRange>> cnf = new LinkedList<List<KeyRange>>();
        boolean hasUnboundedRange = false;
        // Concat byte arrays of literals to form scan start key
        for (KeyExpressionVisitor.KeySlot slot : keySlots) {
            // If the position of the pk columns in the query skips any part of the row k
            // then we have to handle in the next phase through a key filter.
            // If the slot is null this means we have no entry for this pk position.
            if ((slot == null || slot.getPKPosition() != pkPos + 1)) {
                if (!forcedSkipScanFilter) {
                    break;
                }
                if (slot == null) {
                    cnf.add(Collections.singletonList(KeyRange.EVERYTHING_RANGE));
                    continue;
                } else {
                    
                    int limit = table.getBucketNum() == null ? slot.getPKPosition() : slot.getPKPosition() - 1;
                    for (int i=0; i<limit; i++) {
                        cnf.add(Collections.singletonList(KeyRange.EVERYTHING_RANGE));
                    }
                }
            }
            KeyPart keyPart = slot.getKeyPart();
            pkPos = slot.getPKPosition();
            cnf.add(slot.getKeyRanges());
            for (KeyRange range : slot.getKeyRanges()) {
                hasUnboundedRange |= range.isUnbound();
            }
            
            // Will be null in cases for which only part of the expression was factored out here
            // to set the start/end key. An example would be <column> LIKE 'foo%bar' where we can
            // set the start key to 'foo' but still need to match the regex at filter time.
            List<Expression> nodesToExtract = keyPart.getExtractNodes();
            extractNodes.addAll(nodesToExtract);
            // Stop building start/stop key once we encounter a non single key range.
            // TODO: remove this soon after more testing on SkipScanFilter
            if (hasUnboundedRange) {
                // TODO: when stats are available, we may want to continue this loop if the
                // cardinality of this slot is low. We could potentially even continue this
                // loop in the absence of a range for a key slot.
                break;
            }
        }
        RowKeySchema schema = table.getRowKeySchema();
        List<List<KeyRange>> ranges = cnf;
        if (table.getBucketNum() != null) {
            if (!cnf.isEmpty()) {
                // If we have all single keys, we can optimize by adding the salt byte up front
                if (ScanUtil.isAllSingleRowScan(cnf, table.getRowKeySchema())) {
                    cnf.addFirst(SALT_PLACEHOLDER);
                    ranges = SaltingUtil.flattenRanges(cnf, table.getRowKeySchema(), table.getBucketNum());
                    schema = SaltingUtil.VAR_BINARY_SCHEMA;
                } else {
                    cnf.addFirst(SaltingUtil.generateAllSaltingRanges(table.getBucketNum()));
                }
            }
        }
        context.setScanRanges(ScanRanges.create(ranges, schema));
        return whereClause.accept(new RemoveExtractedNodesVisitor(extractNodes));
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
        public Iterator<Expression> visitEnter(OrExpression node) {
            return node.getChildren().iterator();
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
    }

    /*
     * TODO: We could potentially rewrite simple expressions to move constants to the RHS
     * such that we can form a start/stop key for a scan. For example, rewrite this:
     *     WHEREH a + 1 < 5
     * to this instead:
     *     WHERE a < 5 - 1
     * Currently the first case would not be optimized. This includes other arithmetic
     * operators, CASE statements, and string concatenation.
     */
    public static class KeyExpressionVisitor extends TraverseNoExpressionVisitor<KeyExpressionVisitor.KeySlots> {
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

        private KeySlots andKeySlots(AndExpression andExpression, List<KeySlots> childSlots) {
            int nColumns = table.getPKColumns().size();
            KeySlot[] keySlot = new KeySlot[nColumns];
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
                    KeySlot existing = keySlot[position];
                    if (existing == null) {
                        keySlot[position] = slot;
                    } else {
                        keySlot[position] = existing.intersect(slot);
                        if (keySlot[position] == null) {
                            return DEGENERATE_KEY_PARTS;
                        }
                    }
                }
            }

            List<KeySlot> keySlots = Arrays.asList(keySlot);
            // If we have a salt column, skip that slot because
            // they'll never be an expression contained by it.
            if (table.getBucketNum() != null) {
                keySlots = keySlots.subList(1, keySlots.size());
            }
            return new MultiKeySlot(keySlots);
        }

        private KeySlots orKeySlots(OrExpression orExpression, List<KeySlots> childSlots) {
            // If any children were filtered out, filter out the entire
            // OR expression because we don't have enough information to
            // constraint the scan start/stop key. An example would be:
            // WHERE organization_id=? OR key_value_column = 'x'
            // In this case, we cannot simply filter the key_value_column,
            // because we end up bubbling up only the organization_id=?
            // expression to form the start/stop key which is obviously wrong.
            // For an OR expression, you need to be able to extract
            // everything or nothing.
            if (orExpression.getChildren().size() != childSlots.size()) {
                return null;
            }
            KeySlot theSlot = null;
            List<KeyRange> union = Lists.newArrayList();
            for (KeySlots childSlot : childSlots) {
                if (childSlot == DEGENERATE_KEY_PARTS) {
                    // TODO: can this ever happen and can we safely filter the expression tree?
                    continue;
                }
                for (KeySlot slot : childSlot) {
                    // We have a nested OR with nothing for this slot, so continue
                    if (slot == null) {
                        continue;
                    }
                    /*
                     * If we see a different PK column than before, we can't
                     * optimize it because our SkipScanFilter only handles
                     * top level expressions that are ANDed together (where in
                     * the same column expressions may be ORed together).
                     * For example, WHERE a=1 OR b=2 cannot be handled, while
                     *  WHERE (a=1 OR a=2) AND (b=2 OR b=3) can be handled.
                     * TODO: We could potentially handle these cases through
                     * multiple, nested SkipScanFilters, where each OR expression
                     * is handled by its own SkipScanFilter and the outer one
                     * increments the child ones and picks the one with the smallest
                     * key.
                     */
                    if (theSlot == null) {
                        theSlot = slot;
                    } else if (theSlot.getPKPosition() != slot.getPKPosition()) {
                        return null;
                    }
                    union.addAll(slot.getKeyRanges());
                }
            }

            return theSlot == null ? null : newKeyParts(theSlot, orExpression, KeyRange.coalesce(union));
        }

        private final PTable table;

        public KeyExpressionVisitor(PTable table) {
            this.table = table;
        }

        @Override
        public KeySlots defaultReturn(Expression node, List<KeySlots> l) {
            // Passes the CompositeKeyExpression up the tree
            return l.size() == 1 ? l.get(0) : null;
        }


        // TODO: same visitEnter/visitLeave for OrExpression once we optimize it
        @Override
        public Iterator<Expression> visitEnter(AndExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public KeySlots visitLeave(AndExpression node, List<KeySlots> l) {
            KeySlots keyExpr = andKeySlots(node, l);
            return keyExpr;
        }

        @Override
        public Iterator<Expression> visitEnter(OrExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public KeySlots visitLeave(OrExpression node, List<KeySlots> l) {
            KeySlots keySlots = orKeySlots(node, l);
            if (keySlots == null) {
                // If we don't clear the child list, we end up passing some of
                // the child expressions of the OR up the tree, causing only
                // those expressions to form the scan start/stop key.
                l.clear();
                return null;
            }
            return keySlots;
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
            KeyPart childPart = childSlot.getKeyPart();
            ColumnModifier modifier = childPart.getColumn().getColumnModifier();
            CompareOp op = node.getFilterOp();
            // For descending columns, the operator needs to be transformed to
            // it's opposite, since the range is backwards.
            if (modifier != null) {
                op = modifier.transform(op);
            }
            KeyRange keyRange = childPart.getKeyRange(op, key);
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

            return Iterators.singletonIterator(node.getChildren().get(0));
        }

        @Override
        public KeySlots visitLeave(LikeExpression node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }
            // for SUBSTR(<column>,1,3) LIKE 'foo%'
            KeySlot childSlot = childParts.get(0).iterator().next();
            final String startsWith = node.getLiteralPrefix();
            byte[] key = PDataType.CHAR.toBytes(startsWith, node.getChildren().get(0).getColumnModifier());
            // If the expression is an equality expression against a fixed length column
            // and the key length doesn't match the column length, the expression can
            // never be true.
            // An zero length byte literal is null which can never be compared against as true
            Integer childNodeFixedLength = node.getChildren().get(0).getByteSize();
            if (childNodeFixedLength != null && key.length > childNodeFixedLength) {
                return DEGENERATE_KEY_PARTS;
            }
            // TODO: is there a case where we'd need to go through the childPart to calculate the key range?
            PColumn column = childSlot.getKeyPart().getColumn();
            PDataType type = column.getDataType();
            KeyRange keyRange = type.getKeyRange(key, true, ByteUtil.nextKey(key), false);
            Integer columnFixedLength = column.getByteSize();
            if (columnFixedLength != null) {
                keyRange = keyRange.fill(columnFixedLength);
            }
            // Only extract LIKE expression if pattern ends with a wildcard and everything else was extracted
            return newKeyParts(childSlot, node.endsWithOnlyWildcard() ? node : null, keyRange);
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
            PColumn column = childSlot.getKeyPart().getColumn();
            PDataType type = column.getDataType();
            boolean isFixedWidth = type.isFixedWidth();
            if (isFixedWidth) { // if column can't be null
                return node.isNegate() ? null : 
                    newKeyParts(childSlot, node, type.getKeyRange(new byte[column.getByteSize()], true,
                                                                  KeyRange.UNBOUND, true));
            } else {
                KeyRange keyRange = node.isNegate() ? KeyRange.IS_NOT_NULL_RANGE : KeyRange.IS_NULL_RANGE;
                return newKeyParts(childSlot, node, keyRange);
            }
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
            
            private SingleKeySlot(KeySlot slot) {
                this.slot = slot;
            }
            
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
                // If the column is fixed width, fill is up to it's byte size
                PDataType type = getColumn().getDataType();
                if (type.isFixedWidth()) {
                    Integer length = getColumn().getByteSize();
                    if (length != null) {
                        key = ByteUtil.fillKey(key, length);
                    }
                }
                switch (op) {
                case EQUAL:
                    return type.getKeyRange(key, true, key, true);
                case GREATER:
                    return type.getKeyRange(key, false, KeyRange.UNBOUND, false);
                case GREATER_OR_EQUAL:
                    return type.getKeyRange(key, true, KeyRange.UNBOUND, false);
                case LESS:
                    return type.getKeyRange(KeyRange.UNBOUND, false, key, false);
                case LESS_OR_EQUAL:
                    return type.getKeyRange(KeyRange.UNBOUND, false, key, true);
                default:
                    throw new IllegalArgumentException("Unknown operator " + op);
                }
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
