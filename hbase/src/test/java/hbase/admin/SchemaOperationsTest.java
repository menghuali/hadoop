package hbase.admin;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SchemaOperationsTest {
	private static TableName name = TableName.valueOf("table");
	private static Connection conn;
	private static HBaseTestingUtility util;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		util = new HBaseTestingUtility();
		util.startMiniCluster();
		conn = util.getConnection();
		HTableDescriptor htd = new HTableDescriptor(name);
		HColumnDescriptor hcd = new HColumnDescriptor("cf");
		htd.addFamily(hcd);
		Admin admin = conn.getAdmin();
		admin.createTable(htd);
		admin.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		conn.close();
	}

	@Test
	public void test() throws IOException {
		Admin admin = conn.getAdmin();
		HTableDescriptor htd = admin.getTableDescriptor(name);
		Collection<HColumnDescriptor> families = htd.getFamilies();
		assertEquals(1, families.size());
		assertEquals("Familiy name isn't 'cf'", "cf", families.iterator().next().getNameAsString());

		admin.disableTable(name);
		admin.addColumn(name, new HColumnDescriptor("cf2"));
		admin.deleteColumn(name, Bytes.toBytes("cf"));
		admin.enableTable(name);

		htd = admin.getTableDescriptor(name);
		families = htd.getFamilies();
		assertEquals(1, families.size());
		assertEquals("Familiy name isn't 'cf2'", "cf2", families.iterator().next().getNameAsString());

	}

}
