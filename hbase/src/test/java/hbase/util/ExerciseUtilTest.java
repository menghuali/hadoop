package hbase.util;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExerciseUtilTest {

	private static HBaseTestingUtility hbtUtil;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		hbtUtil = new HBaseTestingUtility();
		hbtUtil.startMiniCluster();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetEnv() {
		assertEquals("/opt/hadoop-2.7.1/etc/hadoop", ExerciseUtil.getEnv("HADOOP_CONF_DIR"));
		assertEquals("/opt/hbase-1.2.2/conf", ExerciseUtil.getEnv("HBASE_CONF_DIR"));
	}

	@Test
	public void testGetDefaultHBaseConfiguration() {
		Configuration conf = ExerciseUtil.getDefaultHBaseConfiguration();
		assertEquals("hdfs://localhost:9000", conf.get("fs.defaultFS"));
		assertEquals("hdfs://localhost:9000/hbase", conf.get("hbase.rootdir"));
	}

	@Test
	public void testResetDefaultExerciseTable() throws Throwable {
		Connection conn = null;
		try {
			conn = hbtUtil.getConnection();
			ExerciseUtil.resetDefaultExerciseTable(conn);

			Table table = conn.getTable(ExerciseUtil.DEFAULT_TABLE_NAME);
			assertNotNull(table);

			HTableDescriptor desc = table.getTableDescriptor();
			assertNotNull(desc);

			assertNotNull(desc.getFamily(ExerciseUtil.DEFAULT_CF_BIN1));
			assertNotNull(desc.getFamily(ExerciseUtil.DEFAULT_CF_BIN2));
		} finally {
			if (conn != null)
				conn.close();
		}
	}

}
