package com.salesforce.phoenix.exception;

import java.io.IOException;
import java.sql.SQLException;

public class PhoenixIOException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCodeEnum code = SQLExceptionCodeEnum.IO_EXCEPTION;

    public PhoenixIOException(IOException e) {
        super(new SQLExceptionInfo.Builder(SQLExceptionCodeEnum.IO_EXCEPTION).setRootCause(e).build().toString(), code.getSQLState(), e);
    }

}
