package com.salesforce.phoenix.parse;

public class ShowTablesStatement implements BindableStatement {
    @Override
    public int getBindCount() {
        return 0;
    }
}
