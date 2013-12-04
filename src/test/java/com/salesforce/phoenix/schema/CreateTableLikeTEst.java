package com.salesforce.phoenix.schema;

import com.salesforce.phoenix.jdbc.PhoenixDriver;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created with IntelliJ IDEA.
 * User: haitao.yao
 * Date: 13-12-4
 * Time: 上午10:16
 */
public class CreateTableLikeTest {

    private static final String URL = "jdbc:phoenix:test";

    @BeforeClass
    public static void beforeClass(){
       new PhoenixDriver();
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL);
    }

    @Test
    public void testCreateTableLike() throws SQLException {
        Connection conn = this.getConnection();
//        conn.createStatement().execute("select * from test1 limit 1");
        String createTableLikeSQL = "create table test6 like test1";
        Statement statement = conn.createStatement();
        statement.execute(createTableLikeSQL);
    }

}
