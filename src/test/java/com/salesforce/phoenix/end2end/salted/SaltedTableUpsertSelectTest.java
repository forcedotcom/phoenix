package com.salesforce.phoenix.end2end.salted;

import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.end2end.BaseHBaseManagedTimeTest;


public class SaltedTableUpsertSelectTest extends BaseHBaseManagedTimeTest {

    @Test
    public void testUpsertIntoSaltedTableFromNormalTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertIntoNormalTableFromSaltedTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSaltedTableIntoSaltedTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            
        } finally {
            conn.close();
        }
    }
}
