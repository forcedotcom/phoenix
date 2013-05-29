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

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.salesforce.phoenix.exception.UnknownFunctionException;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.expression.function.*;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import com.salesforce.phoenix.parse.JoinTableNode.JoinType;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 *
 * Factory used by parser to construct object model while parsing a SQL statement
 *
 * @author jtaylor
 * @since 0.1
 */
public class ParseNodeFactory {
    // TODO: Use Google's Reflection library instead to find aggregate functions
    @SuppressWarnings("unchecked")
    private static final List<Class<? extends FunctionExpression>> CLIENT_SIDE_BUILT_IN_FUNCTIONS = Arrays.<Class<? extends FunctionExpression>>asList(
        CurrentDateFunction.class,
        CurrentTimeFunction.class,
        AvgAggregateFunction.class
        );
    private static final Map<BuiltInFunctionKey, BuiltInFunctionInfo> BUILT_IN_FUNCTION_MAP = Maps.newHashMap();

    /**
     *
     * Key used to look up a built-in function using the combination of
     * the lowercase name and the number of arguments. This disambiguates
     * the aggregate MAX(<col>) from the non aggregate MAX(<col1>,<col2>).
     *
     * @author jtaylor
     * @since 0.1
     */
    private static class BuiltInFunctionKey {
        private final String upperName;
        private final int argCount;

        private BuiltInFunctionKey(String lowerName, int argCount) {
            this.upperName = lowerName;
            this.argCount = argCount;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + argCount;
            result = prime * result + ((upperName == null) ? 0 : upperName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            BuiltInFunctionKey other = (BuiltInFunctionKey)obj;
            if (argCount != other.argCount) return false;
            if (!upperName.equals(other.upperName)) return false;
            return true;
        }
    }

    private static void addBuiltInFunction(Class<? extends FunctionExpression> f) throws Exception {
        BuiltInFunction d = f.getAnnotation(BuiltInFunction.class);
        if (d == null) {
            return;
        }
        int nArgs = d.args().length;
        BuiltInFunctionInfo value = new BuiltInFunctionInfo(f, d);
        do {
            // Add function to function map, throwing if conflicts found
            // Add entry for each possible version of function based on arguments that are not required to be present (i.e. arg with default value)
            BuiltInFunctionKey key = new BuiltInFunctionKey(value.getName(), nArgs);
            if (BUILT_IN_FUNCTION_MAP.put(key, value) != null) {
                throw new IllegalStateException("Multiple " + value.getName() + " functions with " + nArgs + " arguments");
            }
        } while (--nArgs >= 0 && d.args()[nArgs].defaultValue().length() > 0);

        // Look for default values that aren't at the end and throw
        while (--nArgs >= 0) {
            if (d.args()[nArgs].defaultValue().length() > 0) {
                throw new IllegalStateException("Function " + value.getName() + " has non trailing default value of '" + d.args()[nArgs].defaultValue() + "'. Only trailing arguments may have default values");
            }
        }
    }
    /**
     * Reflect this class and populate static structures from it.
     * Don't initialize in static block because we have a circular dependency
     */
    private synchronized static void initBuiltInFunctionMap() {
        if (!BUILT_IN_FUNCTION_MAP.isEmpty()) {
            return;
        }
        Class<? extends FunctionExpression> f = null;
        try {
            // Reflection based parsing which yields direct explicit function evaluation at runtime
            for (int i = 0; i < CLIENT_SIDE_BUILT_IN_FUNCTIONS.size(); i++) {
                f = CLIENT_SIDE_BUILT_IN_FUNCTIONS.get(i);
                addBuiltInFunction(f);
            }
            for (ExpressionType et : ExpressionType.values()) {
                Class<? extends Expression> ec = et.getExpressionClass();
                if (FunctionExpression.class.isAssignableFrom(ec)) {
                    @SuppressWarnings("unchecked")
                    Class<? extends FunctionExpression> c = (Class<? extends FunctionExpression>)ec;
                    addBuiltInFunction(f = c);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed initialization of built-in functions at class '" + f + "'", e);
        }
    }

    private static BuiltInFunctionInfo getInfo(String name, List<ParseNode> children) {
        return get(SchemaUtil.normalizeIdentifier(name), children);
    }

    private static BuiltInFunctionInfo get(String normalizedName, List<ParseNode> children) {
        initBuiltInFunctionMap();
        BuiltInFunctionInfo info = BUILT_IN_FUNCTION_MAP.get(new BuiltInFunctionKey(normalizedName,children.size()));
        if (info == null) {
            throw new UnknownFunctionException(normalizedName);
        }
        return info;
    }

    public ParseNodeFactory() {
    }

    public ExplainStatement explain(SQLStatement statement) {
        return new ExplainStatement(statement);
    }

    public ShowTablesStatement showTables() {
        return new ShowTablesStatement();
    }

    public AliasedNode aliasedNode(String alias, ParseNode expression) {
        return new AliasedNode(alias, expression);
    }

    public AddParseNode add(List<ParseNode> children) {
        return new AddParseNode(children);
    }

    public SubtractParseNode subtract(List<ParseNode> children) {
        return new SubtractParseNode(children);
    }

    public MultiplyParseNode multiply(List<ParseNode> children) {
        return new MultiplyParseNode(children);
    }

    public AndParseNode and(List<ParseNode> children) {
        return new AndParseNode(children);
    }

    public FamilyParseNode family(String familyName){
    	return new FamilyParseNode(familyName);
    }

    public WildcardParseNode wildcard() {
        return WildcardParseNode.INSTANCE;
    }

    public BetweenParseNode between(ParseNode l, ParseNode r1, ParseNode r2, boolean negate) {
        return new BetweenParseNode(l, r1, r2, negate);
    }

    public BindParseNode bind(String bind) {
        return new BindParseNode(bind);
    }

    public StringConcatParseNode concat(List<ParseNode> children) {
        return new StringConcatParseNode(children);
    }

    public ColumnParseNode column(String name) {
        return new ColumnParseNode(name);
    }

    public ColumnParseNode column(TableName tableName, String name) {
        return new ColumnParseNode(tableName,name);
    }
    
    public ColumnDefName columnDefName(String columnName) {
        return new ColumnDefName(columnName);
    }

    public ColumnDefName columnDefName(String familyName, String columnName) {
        return new ColumnDefName(familyName, columnName);
    }

    public PropertyName propertyName(String propertyName) {
        return new PropertyName(propertyName);
    }

    public PropertyName propertyName(String familyName, String propertyName) {
        return new PropertyName(familyName, propertyName);
    }

    public ColumnDef columnDef(ColumnDefName columnDefName, String sqlTypeName, boolean isNull, Integer maxLength, Integer scale, boolean isPK, ColumnModifier columnModifier) throws SQLException {
        return new ColumnDef(columnDefName, sqlTypeName, isNull, maxLength, scale, isPK, columnModifier);
    }

    public PrimaryKeyConstraint primaryKey(String name, List<Pair<String, ColumnModifier>> columnNameAndModifier) {
        return new PrimaryKeyConstraint(name, columnNameAndModifier);
    }
    
    public CreateTableStatement createTable(TableName tableName, ListMultimap<String,Pair<String,Object>> props, List<ColumnDef> columns, PrimaryKeyConstraint pkConstraint, List<ParseNode> splits, boolean readOnly, boolean ifNotExists, int bindCount) {
        return new CreateTableStatement(tableName, props, columns, pkConstraint, splits, readOnly, ifNotExists, bindCount);
    }
    
    public AddColumnStatement addColumn(TableName tableName,  ColumnDef columnDef, boolean ifNotExists, Map<String,Object> props) {
        return new AddColumnStatement(tableName, columnDef, ifNotExists, props);
    }
    
    public DropColumnStatement dropColumn(TableName tableName,  ParseNode columnNode, boolean ifExists) {
        return new DropColumnStatement(tableName, columnNode, ifExists);
    }
    
    public DropTableStatement dropTable(TableName tableName, boolean ifExists, boolean isView) {
        return new DropTableStatement(tableName, ifExists, isView);
    }
    
    public TableName table(String schemaName, String tableName) {
        return new TableName(schemaName,tableName);
    }

    public NamedTableNode namedTable(String alias, TableName name ,List<ColumnDef> dyn_columns) {
        return new NamedTableNode(alias, name,dyn_columns);
    }

    public BindTableNode bindTable(String alias, TableName name) {
        return new BindTableNode(alias, name);
    }

    public CaseParseNode caseWhen(List<ParseNode> children) {
        return new CaseParseNode(children);
    }

    public DivideParseNode divide(List<ParseNode> children) {
        return new DivideParseNode(children);
    }


    public EqualParseNode equal(ParseNode lhs, ParseNode rhs) {
        return new EqualParseNode(lhs, rhs);
    }


    public FunctionParseNode functionDistinct(String name, List<ParseNode> args) {
        throw new UnsupportedOperationException("DISTINCT not supported");
    }

    public FunctionParseNode function(String name, List<ParseNode> args) {
        BuiltInFunctionInfo info = getInfo(name, args);
        Constructor<? extends FunctionParseNode> ctor = info.getNodeCtor();
        if (ctor == null) {
            return info.isAggregate()
            ? new AggregateFunctionParseNode(name, args, info)
            : new FunctionParseNode(name, args, info);
        } else {
            try {
                return ctor.newInstance(name, args, info);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    public GreaterThanParseNode gt(ParseNode lhs, ParseNode rhs) {
        return new GreaterThanParseNode(lhs, rhs);
    }


    public GreaterThanOrEqualParseNode gte(ParseNode lhs, ParseNode rhs) {
        return new GreaterThanOrEqualParseNode(lhs, rhs);
    }


    public HintNode hint(String hint) {
        return new HintNode(hint);
    }


    public InListParseNode inList(List<ParseNode> children, boolean negate) {
        return new InListParseNode(children, negate);
    }

    public ExistsParseNode exists(ParseNode l, ParseNode r, boolean negate) {
        return new ExistsParseNode(l, r, negate);
    }

    public InParseNode in(ParseNode l, ParseNode r, boolean negate) {
        return new InParseNode(l, r, negate);
    }

    public IsNullParseNode isNull(ParseNode child, boolean negate) {
        return new IsNullParseNode(child, negate);
    }

    public JoinTableNode join (String alias, NamedTableNode table, ParseNode on, JoinType type) {
        return new JoinTableNode(alias, table, on, type);
    }

    public DerivedTableNode subselect (String alias, SelectStatement select) {
        return new DerivedTableNode(alias, select);
    }

    public LikeParseNode like(ParseNode lhs, ParseNode rhs, boolean negate) {
        return new LikeParseNode(lhs, rhs, negate);
    }


    public LiteralParseNode literal(Object value) {
        return new LiteralParseNode(value);
    }

    private void checkTypeMatch (PDataType expectedType, PDataType actualType) throws SQLException {
        if (!expectedType.isCoercibleTo(actualType)) {
            throw new TypeMismatchException(expectedType, actualType);
        }
    }

    public LiteralParseNode literal(Object value, PDataType expectedType) throws SQLException {
        PDataType actualType = PDataType.fromLiteral(value);
        if (actualType != null && actualType != expectedType) {
            checkTypeMatch(expectedType, actualType);
            value = expectedType.toObject(value, actualType);
        }
        return new LiteralParseNode(value);
    }

    public LiteralParseNode coerce(LiteralParseNode literalNode, PDataType expectedType) throws SQLException {
        PDataType actualType = literalNode.getType();
        if (actualType != null) {
            Object before = literalNode.getValue();
            checkTypeMatch(expectedType, actualType);
            Object after = expectedType.toObject(before, actualType);
            if (before != after) {
                literalNode = literal(after);
            }
        }
        return literalNode;
    }


    public LessThanParseNode lt(ParseNode lhs, ParseNode rhs) {
        return new LessThanParseNode(lhs, rhs);
    }


    public LessThanOrEqualParseNode lte(ParseNode lhs, ParseNode rhs) {
        return new LessThanOrEqualParseNode(lhs, rhs);
    }


    public MultiplyParseNode negate(ParseNode child) {
        return new MultiplyParseNode(Arrays.asList(child,this.literal(-1)));
    }


    public NotParseNode not(ParseNode child) {
        return new NotParseNode(child);
    }


    public NotEqualParseNode notEqual(ParseNode lhs, ParseNode rhs) {
        return new NotEqualParseNode(lhs, rhs);
    }


    public OrParseNode or(List<ParseNode> children) {
        return new OrParseNode(children);
    }


    public OrderByNode orderBy(ParseNode expression, boolean nullsLast, boolean orderAscending) {
        return new OrderByNode(expression, nullsLast, orderAscending);
    }


    public OuterJoinParseNode outer(ParseNode node) {
        return new OuterJoinParseNode(node);
    }

    public SelectStatement select(List<TableNode> from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
            List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount) {

        return new SelectStatement(from, hint, isDistinct, select, where, groupBy == null ? Collections.<ParseNode>emptyList() : groupBy, having, orderBy == null ? Collections.<OrderByNode>emptyList() : orderBy, limit, bindCount);
    }
    
    public UpsertStatement upsert(TableName table, List<ParseNode> columns, boolean isIncrease, List<ParseNode> values, SelectStatement select, int bindCount) {
        return new UpsertStatement(table, columns, isIncrease, values, select, bindCount);
    }
    
    public DeleteStatement delete(TableName table, HintNode hint, ParseNode node, List<OrderByNode> orderBy, LimitNode limit, int bindCount) {
        return new DeleteStatement(table, hint, node, orderBy, limit, bindCount);
    }

    public SelectStatement select(SelectStatement statement, ParseNode where, ParseNode having) {
        return select(statement.getFrom(), statement.getHint(), statement.isDistinct(), statement.getSelect(), where, statement.getGroupBy(), having, statement.getOrderBy(), statement.getLimit(), statement.getBindCount());
    }

    public SubqueryParseNode subquery(SelectStatement select) {
        return new SubqueryParseNode(select);
    }

    public LimitNode limit(BindParseNode b) {
        return new LimitNode(b);
    }

    public LimitNode limit(LiteralParseNode l) {
        return new LimitNode(l);
    }
}
