package hbase;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class BaseTest {
	/**
	 * The name of the default HBase table for exercise (String)
	 */
	protected static final String DEFAULT_TABLE_NAME_STR = "JUNIT_EXERCISE_TABLE";

	/**
	 * The name of the default HBase table for exercise (byte arry)
	 */
	protected static final byte[] DEFAULT_TABLE_BIN = Bytes.toBytes(DEFAULT_TABLE_NAME_STR);

	/**
	 * The name of the default HBase table for exercise (TableName)
	 */
	protected static final TableName DEFAULT_TABLE_NAME = TableName.valueOf(DEFAULT_TABLE_BIN);

	/**
	 * The 1st column family name of the default HBase table for exercise
	 * (String)
	 */
	protected static final String DEFAULT_CF_NAME1 = "CF1";

	/**
	 * The 1st column family name of the default HBase table for exercise (byte
	 * array)
	 */
	protected static final byte[] DEFAULT_CF_BIN1 = Bytes.toBytes(DEFAULT_CF_NAME1);

	/**
	 * The 2nd column family name of the default HBase table for exercise
	 * (String)
	 */
	protected static final String DEFAULT_CF_NAME2 = "CF2";

	/**
	 * The 2nd column family name of the default HBase table for exercise (byte
	 * array)
	 */
	protected static final byte[] DEFAULT_CF_BIN2 = Bytes.toBytes(DEFAULT_CF_NAME2);

	/**
	 * Logger
	 */
	private static final Logger LOG = Logger.getAnonymousLogger();

	protected static HBaseTestingUtility hbtUtil;
	protected static Connection conn;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		hbtUtil = new HBaseTestingUtility();
		hbtUtil.startMiniCluster();
		conn = hbtUtil.getConnection();
		resetDefaultTable();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		conn.close();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	protected String generateRowKey(Class<?> clazz, String method) {
		return clazz.getCanonicalName() + "#" + method;
	}

	protected static void resetDefaultTable() throws Exception {
		LOG.info("Reset the default HBase table for exercise.");
		Admin admin = null;
		try {
			conn = hbtUtil.getConnection();
			admin = conn.getAdmin();
			if (admin.tableExists(DEFAULT_TABLE_NAME)) {
				LOG.info(DEFAULT_TABLE_NAME_STR + " exists. Deleting it.");
				admin.disableTable(DEFAULT_TABLE_NAME);
				admin.deleteTable(DEFAULT_TABLE_NAME);
			}
			LOG.info("Creating " + DEFAULT_TABLE_NAME_STR);
			HTableDescriptor table = new HTableDescriptor(DEFAULT_TABLE_NAME);
			table.addFamily(new HColumnDescriptor(DEFAULT_CF_BIN1));
			table.addFamily(new HColumnDescriptor(DEFAULT_CF_BIN2));
			admin.createTable(table);
			LOG.info("Done.");
		} catch (Throwable e) {
			throw new RuntimeException("Couldn't reset the default HBase table for exercise.", e);
		} finally {
			if (admin != null)
				try {
					admin.close();
				} catch (Throwable e) {
					LOG.log(Level.WARNING, "Couldn't close HBase admin instance.", e);
				}
		}
	}

}
