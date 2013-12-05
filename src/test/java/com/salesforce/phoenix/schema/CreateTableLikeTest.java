package com.salesforce.phoenix.schema;


import com.salesforce.phoenix.jdbc.PhoenixDriver;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class CreateTableLikeTest {
    @BeforeClass
    public static void beforeClass() throws SQLException{
        new PhoenixDriver();
    }

    private static final String URL = "jdbc:phoenix:test";

    private Connection getConnection() throws SQLException{
        return DriverManager.getConnection(URL);
    }
    @Test
    public void testCreateTableLike() throws SQLException {
        Connection conn = this.getConnection();
        conn.createStatement().execute("create table test10 like test1");

    }

}
