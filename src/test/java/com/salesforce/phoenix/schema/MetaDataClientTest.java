package com.salesforce.phoenix.schema;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.TableName;
import com.salesforce.phoenix.query.BaseTest;
import com.salesforce.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.fail;

/**
 * Created with IntelliJ IDEA.
 * User: haitao.yao
 * Date: 13-12-3
 * Time: 上午10:28
 */
public class MetaDataClientTest extends BaseTest {

    @BeforeClass
    public static void setUp() {
        try {
            startServer(TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL);
        } catch (Exception e) {
            fail("failed to start test server" + e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL);
    }

    @Test
    public void testWriteSchemaInfo() throws SQLException {
        MetaDataClient metaDataClient = new MetaDataClient((PhoenixConnection) this.getConnection());
        TableName baseTable = TableName.create(null, "test1");
        TableName newTable = TableName.create(null, "test2");
    }

    @Test
    public void printSql(){
    }

    @Test
    public void testGetTableKey(){

    }
}
