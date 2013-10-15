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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.expression.AndExpression;
import com.salesforce.phoenix.expression.ComparisonExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.InListExpression;
import com.salesforce.phoenix.expression.IsNullExpression;
import com.salesforce.phoenix.expression.LikeExpression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.OrExpression;
import com.salesforce.phoenix.expression.RowKeyColumnExpression;
import com.salesforce.phoenix.expression.RowValueConstructorExpression;
import com.salesforce.phoenix.expression.function.FunctionExpression.OrderPreserving;
import com.salesforce.phoenix.expression.function.ScalarFunction;
import com.salesforce.phoenix.expression.visitor.TraverseNoExpressionVisitor;
import com.salesforce.phoenix.parse.FilterableStatement;
import com.salesforce.phoenix.parse.HintNode.Hint;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.RowKeySchema;
import com.salesforce.phoenix.schema.SaltingUtil;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.ScanUtil;
import com.salesforce.phoenix.util.SchemaUtil;


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
     * @param statement the statement being compiled
     * @param whereClause the where clause expression
     * @return the new where clause with the key expressions removed
     */
    public static Expression pushKeyExpressionsToScan(StatementContext context, FilterableStatement statement, Expression whereClause) {
        return pushKeyExpressionsToScan(context, statement, whereClause, null);
    }

    // For testing so that the extractedNodes can be verified
    public static Expression pushKeyExpressionsToScan(StatementContext context, FilterableStatement statement,
            Expression whereClause, Set<Expression> extractNodes) {
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
        KeyExpressionVisitor visitor = new KeyExpressionVisitor(context, table);
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

        // We're fully qualified if all columns except the salt column are specified
        int fullyQualifiedColumnCount = table.getPKColumns().size() - (table.getBucketNum() == null ? 0 : 1);
        int pkPos = table.getBucketNum() == null ? -1 : 0;
        LinkedList<List<KeyRange>> cnf = new LinkedList<List<KeyRange>>();
        RowKeySchema schema = table.getRowKeySchema();
        boolean forcedSkipScan = statement.getHint().hasHint(Hint.SKIP_SCAN);
        boolean forcedRangeScan = statement.getHint().hasHint(Hint.RANGE_SCAN);
        boolean hasUnboundedRange = false;
        boolean hasAnyRange = false;
        // Concat byte arrays of literals to form scan start key
        for (KeyExpressionVisitor.KeySlot slot : keySlots) {
            // If the position of the pk columns in the query skips any part of the row k
            // then we have to handle in the next phase through a key filter.
            // If the slot is null this means we have no entry for this pk position.
            if (slot == null || slot.getKeyRanges().isEmpty())  {
                if (!forcedSkipScan) break;
                continue;
            }
            if (slot.getPKPosition() != pkPos + 1) {
                if (!forcedSkipScan) break;
                for (int i=pkPos + 1; i < slot.getPKPosition(); i++) {
                    cnf.add(Collections.singletonList(KeyRange.EVERYTHING_RANGE));
                }
            }
            // We support (a,b) IN ((1,2),(3,4), so in this case we switch to a flattened schema
            if (fullyQualifiedColumnCount > 1 && slot.getPKSpan() == fullyQualifiedColumnCount && slot.getKeyRanges().size() > 1) {
                schema = SchemaUtil.VAR_BINARY_SCHEMA;
            }
            KeyPart keyPart = slot.getKeyPart();
            pkPos = slot.getPKPosition();
            List<KeyRange> keyRanges = slot.getKeyRanges();
            cnf.add(keyRanges);
            for (KeyRange range : keyRanges) {
                hasUnboundedRange |= range.isUnbound();
            }
            
            // Will be null in cases for which only part of the expression was factored out here
            // to set the start/end key. An example would be <column> LIKE 'foo%bar' where we can
            // set the start key to 'foo' but still need to match the regex at filter time.
            // Don't extract expressions if we're forcing a range scan and we've already come
            // across a range for a prior slot. The reason is that we have an inexact range after
            // that, so must filter on the remaining conditions (see issue #467).
            if (!forcedRangeScan || !hasAnyRange) {
                List<Expression> nodesToExtract = keyPart.getExtractNodes();
                extractNodes.addAll(nodesToExtract);
            }
            // Stop building start/stop key once we encounter a non single key range.
            if (hasUnboundedRange && !forcedSkipScan) {
                // TODO: when stats are available, we may want to continue this loop if the
                // cardinality of this slot is low. We could potentially even continue this
                // loop in the absence of a range for a key slot.
                break;
            }
            hasAnyRange |= keyRanges.size() > 1 || (keyRanges.size() == 1 && !keyRanges.get(0).isSingleKey());
        }
        List<List<KeyRange>> ranges = cnf;
        if (table.getBucketNum() != null) {
            if (!cnf.isEmpty()) {
                // If we have all single keys, we can optimize by adding the salt byte up front
                if (schema == SchemaUtil.VAR_BINARY_SCHEMA) {
                    ranges = SaltingUtil.setSaltByte(ranges, table.getBucketNum());
                } else if (ScanUtil.isAllSingleRowScan(cnf, table.getRowKeySchema())) {
                    cnf.addFirst(SALT_PLACEHOLDER);
                    ranges = SaltingUtil.flattenRanges(cnf, table.getRowKeySchema(), table.getBucketNum());
                    schema = SchemaUtil.VAR_BINARY_SCHEMA;
                } else {
                    cnf.addFirst(SaltingUtil.generateAllSaltingRanges(table.getBucketNum()));
                }
            }
        }
        context.setScanRanges(
                ScanRanges.create(ranges, schema, statement.getHint().hasHint(Hint.RANGE_SCAN)),
                keySlots.getMinMaxRange());
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

            @Override
            public KeyRange getMinMaxRange() {
                return null;
            }
        };

        private static boolean isDegenerate(List<KeyRange> keyRanges) {
            return keyRanges == null || keyRanges.size() == 1 && keyRanges.get(0) == KeyRange.EMPTY_RANGE;
        }
        
        private static KeySlots newKeyParts(KeySlot slot, Expression extractNode, KeyRange keyRange) {
            if (keyRange == null) {
                return DEGENERATE_KEY_PARTS;
            }
            
            List<KeyRange> keyRanges = slot.getPKSpan() == 1 ? Collections.<KeyRange>singletonList(keyRange) : EVERYTHING_RANGES;
            KeyRange minMaxRange = slot.getPKSpan() == 1 ? null : keyRange;
            return newKeyParts(slot, extractNode, keyRanges, minMaxRange);
        }

        private static KeySlots newKeyParts(KeySlot slot, Expression extractNode, List<KeyRange> keyRanges, KeyRange minMaxRange) {
            if (isDegenerate(keyRanges)) {
                return DEGENERATE_KEY_PARTS;
            }
            
            List<Expression> extractNodes = extractNode == null || slot.getKeyPart().getExtractNodes().isEmpty()
                  ? Collections.<Expression>emptyList()
                  : Collections.<Expression>singletonList(extractNode);
            return new SingleKeySlot(new BaseKeyPart(slot.getKeyPart().getColumn(), extractNodes), slot.getPKPosition(), slot.getPKSpan(), keyRanges, minMaxRange, slot.getOrderPreserving());
        }

        private static KeySlots newKeyParts(KeySlot slot, List<Expression> extractNodes, List<KeyRange> keyRanges, KeyRange minMaxRange) {
            if (isDegenerate(keyRanges)) {
                return DEGENERATE_KEY_PARTS;
            }
            
            return new SingleKeySlot(new BaseKeyPart(slot.getKeyPart().getColumn(), extractNodes), slot.getPKPosition(), slot.getPKSpan(), keyRanges, minMaxRange, slot.getOrderPreserving());
        }

        private KeySlots newRowValueConstructorKeyParts(RowValueConstructorExpression rvc, List<KeySlots> childSlots) {
            if (childSlots.isEmpty() || rvc.isConstant()) {
                return null;
            }
            
            int initPosition = table.getBucketNum() == null ? 0 : 1;
            int position = initPosition;
            for (KeySlots slots : childSlots) {
                KeySlot keySlot = slots.iterator().next();
                // If columns are not in PK order, then stop iteration
                if (keySlot.getPKPosition() != position) {
                    break;
                }
                position++;
                
                // If we come to a point where we're not preserving order completely
                // then stop. We will never get a NO here, but we might get a YES_IF_LAST
                // if the child expression is only using part of the underlying pk column.
                // (for example, in the case of SUBSTR). In this case, we must stop building
                // the row key constructor at that point.
                assert(keySlot.getOrderPreserving() != OrderPreserving.NO);
                if (keySlot.getOrderPreserving() == OrderPreserving.YES_IF_LAST) {
                    break;
                }
            }
            if (position > initPosition) {
                int span = position - initPosition;
                return new SingleKeySlot(new RowValueConstructorKeyPart(table.getPKColumns().get(initPosition), rvc, span), initPosition, span, EVERYTHING_RANGES);
            }
            return null;
        }

        private static KeySlots newScalarFunctionKeyPart(KeySlot slot, ScalarFunction node) {
            if (isDegenerate(slot.getKeyRanges())) {
                return DEGENERATE_KEY_PARTS;
            }
            KeyPart part = node.newKeyPart(slot.getKeyPart());
            if (part == null) {
                return null;
            }
            
            // Scalar function always returns primitive and never a row value constructor, so span is always 1
            return new SingleKeySlot(part, slot.getPKPosition(), slot.getKeyRanges(), node.preservesOrder());
        }

        private KeySlots andKeySlots(AndExpression andExpression, List<KeySlots> childSlots) {
            int nColumns = table.getPKColumns().size();
            KeySlot[] keySlot = new KeySlot[nColumns];
            KeyRange minMaxRange = KeyRange.EVERYTHING_RANGE;
            List<Expression> minMaxExtractNodes = Lists.<Expression>newArrayList();
            int initPosition = (table.getBucketNum() ==null ? 0 : 1);
            for (KeySlots childSlot : childSlots) {
                if (childSlot == DEGENERATE_KEY_PARTS) {
                    return DEGENERATE_KEY_PARTS;
                }
                if (childSlot.getMinMaxRange() != null) {
                    // TODO: potentially use KeySlot.intersect here. However, we can't intersect the key ranges in the slot
                    // with our minMaxRange, since it spans columns and this would mess up our skip scan.
                    minMaxRange = minMaxRange.intersect(childSlot.getMinMaxRange());
                    for (KeySlot slot : childSlot) {
                        minMaxExtractNodes.addAll(slot.getKeyPart().getExtractNodes());
                    }
                } else {
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
            }

            if (!minMaxExtractNodes.isEmpty()) {
                if (keySlot[initPosition] == null) {
                    keySlot[initPosition] = new KeySlot(new BaseKeyPart(table.getPKColumns().get(initPosition), minMaxExtractNodes), initPosition, 1, EVERYTHING_RANGES, null);
                } else {
                    keySlot[initPosition] = keySlot[initPosition].concatExtractNodes(minMaxExtractNodes);
                }
            }
            List<KeySlot> keySlots = Arrays.asList(keySlot);
            // If we have a salt column, skip that slot because
            // they'll never be an expression contained by it.
            keySlots = keySlots.subList(initPosition, keySlots.size());
            return new MultiKeySlot(keySlots, minMaxRange == KeyRange.EVERYTHING_RANGE ? null : minMaxRange);
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
            int initialPos = (table.getBucketNum() == null ? 0 : 1);
            KeySlot theSlot = null;
            List<Expression> slotExtractNodes = Lists.<Expression>newArrayList();
            int thePosition = -1;
            boolean extractAll = true;
            // TODO: Have separate list for single span versus multi span
            // For multi-span, we only need to keep a single range.
            List<KeyRange> slotRanges = Lists.newArrayList();
            KeyRange minMaxRange = KeyRange.EMPTY_RANGE;
            for (KeySlots childSlot : childSlots) {
                if (childSlot == DEGENERATE_KEY_PARTS) {
                    // TODO: can this ever happen and can we safely filter the expression tree?
                    continue;
                }
                if (childSlot.getMinMaxRange() != null) {
                    if (!slotRanges.isEmpty() && thePosition != initialPos) { // ORing together rvc in initial slot with other slots
                        return null;
                    }
                    minMaxRange = minMaxRange.union(childSlot.getMinMaxRange());
                    thePosition = initialPos;
                    for (KeySlot slot : childSlot) {
                        List<Expression> extractNodes = slot.getKeyPart().getExtractNodes();
                        extractAll &= !extractNodes.isEmpty();
                        slotExtractNodes.addAll(extractNodes);
                    }
                } else {
                    // TODO: Do the same optimization that we do for IN if the childSlots specify a fully qualified row key
                    for (KeySlot slot : childSlot) {
                        // We have a nested OR with nothing for this slot, so continue
                        if (slot == null) {
                            continue; // FIXME: I don't think this is ever necessary
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
                        if (thePosition == -1) {
                            theSlot = slot;
                            thePosition = slot.getPKPosition();
                        } else if (thePosition != slot.getPKPosition()) {
                            return null;
                        }
                        List<Expression> extractNodes = slot.getKeyPart().getExtractNodes();
                        extractAll &= !extractNodes.isEmpty();
                        slotExtractNodes.addAll(extractNodes);
                        slotRanges.addAll(slot.getKeyRanges());
                    }
                }
            }

            if (thePosition == -1) {
                return null;
            }
            // With a mix of both, we can't use skip scan, so empty out the union
            // and only extract the min/max nodes.
            if (!slotRanges.isEmpty() && minMaxRange != KeyRange.EMPTY_RANGE) {
                boolean clearExtracts = false;
                // Union the minMaxRanges together with the slotRanges.
                for (KeyRange range : slotRanges) {
                    if (!clearExtracts) {
                        /*
                         * Detect when to clear the extract nodes by determining if there
                         * are gaps left by combining the ranges. If there are gaps, we
                         * cannot extract the nodes, but must them as filters instead.
                         */
                        KeyRange intersection = minMaxRange.intersect(range);
                        if (intersection == KeyRange.EMPTY_RANGE 
                                || !range.equals(intersection.union(range)) 
                                || !minMaxRange.equals(intersection.union(minMaxRange))) {
                            clearExtracts = true;
                        }
                    }
                    minMaxRange = minMaxRange.union(range);
                }
                if (clearExtracts) {
                    extractAll = false;
                    slotExtractNodes = Collections.emptyList();
                }
                slotRanges = Collections.emptyList();
            }
            if (theSlot == null) {
                theSlot = new KeySlot(new BaseKeyPart(table.getPKColumns().get(initialPos), slotExtractNodes), initialPos, 1, EVERYTHING_RANGES, null);
            } else if (minMaxRange != KeyRange.EMPTY_RANGE && !slotExtractNodes.isEmpty()) {
                theSlot = theSlot.concatExtractNodes(slotExtractNodes);
            }
            return newKeyParts(
                    theSlot, 
                    extractAll ? Collections.<Expression>singletonList(orExpression) : slotExtractNodes, 
                    slotRanges.isEmpty() ? EVERYTHING_RANGES : KeyRange.coalesce(slotRanges), 
                    minMaxRange == KeyRange.EMPTY_RANGE ? null : minMaxRange);
        }

        private final PTable table;
        private final StatementContext context;

        public KeyExpressionVisitor(StatementContext context, PTable table) {
            this.context = context;
            this.table = table;
        }

        private boolean isFullyQualified(int pkSpan) {
            int nPKColumns = table.getPKColumns().size();
            return table.getBucketNum() == null ? pkSpan == nPKColumns : pkSpan == nPKColumns-1;
        }
        @Override
        public KeySlots defaultReturn(Expression node, List<KeySlots> l) {
            // Passes the CompositeKeyExpression up the tree
            return l.size() == 1 ? l.get(0) : null;
        }


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
        public Iterator<Expression> visitEnter(RowValueConstructorExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public KeySlots visitLeave(RowValueConstructorExpression node, List<KeySlots> childSlots) {
            return newRowValueConstructorKeyParts(node, childSlots);
        }

        @Override
        public KeySlots visit(RowKeyColumnExpression node) {
            PColumn column = table.getPKColumns().get(node.getPosition());
            return new SingleKeySlot(new BaseKeyPart(column, Collections.<Expression>singletonList(node)), node.getPosition(), 1, EVERYTHING_RANGES);
        }

        @Override
        public Iterator<Expression> visitEnter(ComparisonExpression node) {
            Expression rhs = node.getChildren().get(1);
            if (!rhs.isConstant() || node.getFilterOp() == CompareOp.NOT_EQUAL) {
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
            Expression rhs = node.getChildren().get(1);
            KeySlots childSlots = childParts.get(0);
            KeySlot childSlot = childSlots.iterator().next();
            KeyPart childPart = childSlot.getKeyPart();
            ColumnModifier modifier = childPart.getColumn().getColumnModifier();
            CompareOp op = node.getFilterOp();
            // For descending columns, the operator needs to be transformed to
            // it's opposite, since the range is backwards.
            if (modifier != null) {
                op = modifier.transform(op);
            }
            KeyRange keyRange = childPart.getKeyRange(op, rhs);
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
            KeySlots childSlots = childParts.get(0);
            KeySlot childSlot = childSlots.iterator().next();
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

        @Override
        public KeySlots visitLeave(InListExpression node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }

            List<Expression> keys = node.getKeys();
            List<KeyRange> ranges = Lists.newArrayListWithExpectedSize(keys.size());
            KeySlot childSlot = childParts.get(0).iterator().next();
            KeyPart childPart = childSlot.getKeyPart();
            // We can only optimize a row value constructor that is fully qualified
            if (childSlot.getPKSpan() > 1 && !isFullyQualified(childSlot.getPKSpan())) {
                return newKeyParts(childSlot, (Expression)null, Collections.singletonList(
                        KeyRange.getKeyRange(
                                ByteUtil.copyKeyBytesIfNecessary(node.getMinKey()), true,
                                ByteUtil.copyKeyBytesIfNecessary(node.getMaxKey()), true)), null);
            }
            // Handles cases like WHERE substr(foo,1,3) IN ('aaa','bbb')
            for (Expression key : keys) {
                ranges.add(childPart.getKeyRange(CompareOp.EQUAL, key));
            }
            return newKeyParts(childSlot, node, ranges, null);
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
            KeySlots childSlots = childParts.get(0);
            KeySlot childSlot = childSlots.iterator().next();
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
            public KeyRange getMinMaxRange();
        }

        private static final class KeySlot {
            private final int pkPosition;
            private final int pkSpan;
            private final KeyPart keyPart;
            private final List<KeyRange> keyRanges;
            private final OrderPreserving orderPreserving;

            private KeySlot(KeyPart keyPart, int pkPosition, int pkSpan, List<KeyRange> keyRanges) {
                this (keyPart, pkPosition, pkSpan, keyRanges, OrderPreserving.YES);
            }
            
            private KeySlot(KeyPart keyPart, int pkPosition, int pkSpan, List<KeyRange> keyRanges, OrderPreserving orderPreserving) {
                this.pkPosition = pkPosition;
                this.pkSpan = pkSpan;
                this.keyPart = keyPart;
                this.keyRanges = keyRanges;
                this.orderPreserving = orderPreserving;
            }

            public KeyPart getKeyPart() {
                return keyPart;
            }

            public int getPKPosition() {
                return pkPosition;
            }

            public int getPKSpan() {
                return pkSpan;
            }

            public List<KeyRange> getKeyRanges() {
                return keyRanges;
            }

            public final KeySlot concatExtractNodes(List<Expression> extractNodes) {
                return new KeySlot(
                        new BaseKeyPart(this.getKeyPart().getColumn(),
                                    SchemaUtil.concat(this.getKeyPart().getExtractNodes(),extractNodes)),
                        this.getPKPosition(),
                        this.getPKSpan(),
                        this.getKeyRanges(),
                        this.getOrderPreserving());
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
                        this.getPKSpan(),
                        keyRanges,
                        this.getOrderPreserving());
            }

            public OrderPreserving getOrderPreserving() {
                return orderPreserving;
            }
        }

        private static class MultiKeySlot implements KeySlots {
            private final List<KeySlot> childSlots;
            private final KeyRange minMaxRange;

            private MultiKeySlot(List<KeySlot> childSlots, KeyRange minMaxRange) {
                this.childSlots = childSlots;
                this.minMaxRange = minMaxRange;
            }

            @Override
            public Iterator<KeySlot> iterator() {
                return childSlots.iterator();
            }

            @Override
            public KeyRange getMinMaxRange() {
                return minMaxRange;
            }
        }

        private static class SingleKeySlot implements KeySlots {
            private final KeySlot slot;
            private final KeyRange minMaxRange;
            
            private SingleKeySlot(KeyPart part, int pkPosition, List<KeyRange> ranges) {
                this(part, pkPosition, 1, ranges);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, List<KeyRange> ranges, OrderPreserving orderPreserving) {
                this(part, pkPosition, 1, ranges, orderPreserving);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, int pkSpan, List<KeyRange> ranges) {
                this(part,pkPosition,pkSpan,ranges, null, null);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, int pkSpan, List<KeyRange> ranges, OrderPreserving orderPreserving) {
                this(part,pkPosition,pkSpan,ranges, null, orderPreserving);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, int pkSpan, List<KeyRange> ranges, KeyRange minMaxRange, OrderPreserving orderPreserving) {
                this.slot = new KeySlot(part, pkPosition, pkSpan, ranges, orderPreserving);
                this.minMaxRange = minMaxRange;
            }
            
            @Override
            public Iterator<KeySlot> iterator() {
                return Iterators.<KeySlot>singletonIterator(slot);
            }

            @Override
            public KeyRange getMinMaxRange() {
                return minMaxRange;
            }
            
        }
        
        private static class BaseKeyPart implements KeyPart {
            @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                rhs.evaluate(null, ptr);
                byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
                // If the column is fixed width, fill is up to it's byte size
                PDataType type = getColumn().getDataType();
                if (type.isFixedWidth()) {
                    Integer length = getColumn().getByteSize();
                    if (length != null) {
                        key = ByteUtil.fillKey(key, length);
                    }
                }
                return ByteUtil.getKeyRange(key, op, type);
            }

            private final PColumn column;
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
        
        private  class RowValueConstructorKeyPart implements KeyPart {
            private final RowValueConstructorExpression rvc;
            private final PColumn column;
            private final List<Expression> nodes;

            private RowValueConstructorKeyPart(PColumn column, RowValueConstructorExpression rvc, int span) {
                this.column = column;
                if (span == rvc.getChildren().size()) {
                    this.rvc = rvc;
                    this.nodes = Collections.<Expression>singletonList(rvc);
                } else {
                    this.rvc = new RowValueConstructorExpression(rvc.getChildren().subList(0, span),rvc.isConstant());
                    this.nodes = Collections.<Expression>emptyList();
                }
            }

            @Override
            public List<Expression> getExtractNodes() {
                return nodes;
            }

            @Override
            public PColumn getColumn() {
                return column;
            }
           @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                // Since row value constructors in equality expressions are always rewritten
                // to be simple equality expressions, we know that equality here is for an
                // IN (1,2,3) expression. In that case, we want to use the regular key part
                // logic, as the rhs is not a row value constructor.
                if (op != CompareOp.EQUAL) {
                    boolean usedAllOfLHS = !nodes.isEmpty();
                    // Need special case for a single valued lhs row value constructor (only when
                    // we're extracting it), as this may subtly affect the CompareOp we need
                    // to use. For example: a < (1,2) is true if a = 1, so we need to switch
                    // the compare op to <= like this: a <= 1. Since we strip trailing nulls
                    // in the rvc, we don't need to worry about the a < (1,null) case.
                    if (usedAllOfLHS && rvc.getChildren().size() < rhs.getChildren().size()) {
                        if (op == CompareOp.LESS) {
                            op = CompareOp.LESS_OR_EQUAL;
                        } else if (op == CompareOp.GREATER_OR_EQUAL) {
                            op = CompareOp.GREATER;
                        }
                    }
                    if (!usedAllOfLHS || rvc.getChildren().size() != rhs.getChildren().size()) {
                        // We know that rhs was converted to a row value constructor and that it's a constant
                        rhs= new RowValueConstructorExpression(rhs.getChildren().subList(0, Math.min(rvc.getChildren().size(), rhs.getChildren().size())), rhs.isConstant());
                    }
                }
                ImmutableBytesWritable ptr = context.getTempPtr();
                if (!rhs.evaluate(null, ptr) || ptr.getLength()==0) {
                    return null; 
                }
                byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
                return ByteUtil.getKeyRange(key, op, PDataType.VARBINARY);
            }

        }
    }
}
