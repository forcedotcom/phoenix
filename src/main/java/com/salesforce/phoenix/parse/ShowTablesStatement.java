package com.salesforce.phoenix.parse;

public class ShowTablesStatement implements SQLStatement {
    @Override
    public int getBindCount() {
        return 0;
    }
}
