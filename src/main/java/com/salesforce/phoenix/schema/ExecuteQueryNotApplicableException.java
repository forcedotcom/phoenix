package com.salesforce.phoenix.schema;

import java.sql.SQLException;

import com.salesforce.phoenix.exception.SQLExceptionCodeEnum;
import com.salesforce.phoenix.exception.SQLExceptionInfo;

public class ExecuteQueryNotApplicableException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCodeEnum code = SQLExceptionCodeEnum.EXECUTE_QUERY_NOT_APPLICABLE;

    public ExecuteQueryNotApplicableException(String query) {
        super(new SQLExceptionInfo.Builder(code).setMessage("Query: " + query).build().toString(), code.getSQLState());
    }

    public ExecuteQueryNotApplicableException(String command, String statement) {
        super(new SQLExceptionInfo.Builder(code).setMessage("Command: " + command + ". Statement: " + statement).build().toString(), code.getSQLState());
    }

}
