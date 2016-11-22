/**
 * Test the base and table operations of HBase admin
 */
package hbase.admin;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author mli
 *
 */
public class BaseAndTableOperationsTest {
	private static HBaseTestingUtility util;
	private static Connection conn;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		util = new HBaseTestingUtility();
		util.startMiniCluster();
		conn = util.getConnection();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void basicOperationas() throws IOException {
		Connection conn = null;
		Admin admin = null;
		try {
			conn = util.getConnection();
			admin = conn.getAdmin();
			admin = conn.getAdmin();
			assertNotNull(admin.getConnection());
			assertNotNull(admin.getConfiguration());
		} finally {
			if (admin != null)
				admin.close();
		}
	}

	@Test
	public void creatTable_1() throws IOException {
		Admin admin = null;
		try {
			// Create table
			TableName tname = TableName.valueOf("table1");
			HTableDescriptor tdesc = new HTableDescriptor(tname);
			HColumnDescriptor cf = new HColumnDescriptor("cf");
			tdesc.addFamily(cf);
			conn = util.getConnection();
			admin = conn.getAdmin();
			admin.createTable(tdesc); // end

			// Get table and test its descriptor
			Table table = conn.getTable(tname);
			assertNotNull(table);
			HTableDescriptor tdesc2 = table.getTableDescriptor();
			assertEquals(tdesc, tdesc2);
		} finally {
			if (admin != null)
				admin.close();
		}
	}

	@Test
	public void creatTable_2() throws IOException, InterruptedException {
		Admin admin = null;
		try {
			// Create table
			TableName tname = TableName.valueOf("table2");
			HTableDescriptor tdesc = new HTableDescriptor(tname);
			HColumnDescriptor cf = new HColumnDescriptor("cf");
			tdesc.addFamily(cf);
			conn = util.getConnection();
			admin = conn.getAdmin();
			admin.createTable(tdesc, new byte[][] { Bytes.toBytes("A"), Bytes.toBytes("B"), Bytes.toBytes("C") }); // end

			// Get table and test its descriptor
			assertTrue(admin.isTableAvailable(tname));
			Table table = conn.getTable(tname);
			assertNotNull(table);
			HTableDescriptor tdesc2 = table.getTableDescriptor();
			assertEquals(tdesc, tdesc2);
		} finally {
			if (admin != null)
				admin.close();
		}
	}

	@Test
	public void listTables() throws IOException {
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			HTableDescriptor[] descs = admin.listTables();
			for (HTableDescriptor desc : descs) {
				admin.disableTable(desc.getTableName());
				admin.deleteTable(desc.getTableName());
			}

			TableName name1 = TableName.valueOf("listTables_1");
			TableName name2 = TableName.valueOf("listTables_2");
			admin.createTable((new HTableDescriptor(name1)).addFamily(new HColumnDescriptor("cf1")));
			admin.createTable((new HTableDescriptor(name2)).addFamily(new HColumnDescriptor("cf2")));

			assertTrue(admin.isTableAvailable(name1));
			assertTrue(admin.isTableAvailable(name2));

			descs = admin.listTables();
			assertEquals("The table descritpor number isn't 2", 2, descs.length);
		} finally {
			if (admin != null)
				admin.close();
		}
	}

	@Test
	public void modifyTable() throws IOException {
		Admin admin = null;
		try {
			// create table
			admin = conn.getAdmin();
			TableName name = TableName.valueOf(Bytes.toBytes("modify_table"));
			HTableDescriptor htd1 = new HTableDescriptor(name);
			admin.createTable(htd1.addFamily(new HColumnDescriptor(Bytes.toBytes("cf"))));
			assertTrue("'modify_table' isn't enabled", admin.isTableEnabled(name));
			
			// modify table descriptor
			admin.disableTable(name);
			HTableDescriptor htd2 = new HTableDescriptor(name);
			admin.modifyTable(name, htd2.addFamily(new HColumnDescriptor(Bytes.toBytes("cf2"))));
			admin.enableTable(name);
			HTableDescriptor htd3 = admin.getTableDescriptor(name);
			assertEquals(
					"The new descriptor of 'modify_table' doesn't equal to the one which is used to modify this talbe",
					htd3, htd2);
			assertNotEquals("The new descriptor of 'modify_table' equals the descriptor before modification", htd3,
					htd1);
		} finally {
			if (admin != null)
				admin.close();
		}
	}

}
