/**
 * Utility class for HBase exercise
 */
package hbase.util;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author mli
 *
 */
public final class ExerciseUtil {

	/**
	 * The name of the default HBase table for exercise (String)
	 */
	public static final String DEFAULT_TABLE_NAME_STR = "DEFAULT_EXERCISE_TABLE";

	/**
	 * The name of the default HBase table for exercise (byte arry)
	 */
	public static final byte[] DEFAULT_TABLE_BIN = Bytes.toBytes(DEFAULT_TABLE_NAME_STR);

	/**
	 * The name of the default HBase table for exercise (TableName)
	 */
	public static final TableName DEFAULT_TABLE_NAME = TableName.valueOf(DEFAULT_TABLE_BIN);

	/**
	 * The 1st column family name of the default HBase table for exercise
	 * (String)
	 */
	public static final String DEFAULT_CF_NAME1 = "CF1";

	/**
	 * The 1st column family name of the default HBase table for exercise (byte
	 * array)
	 */
	public static final byte[] DEFAULT_CF_BIN1 = Bytes.toBytes(DEFAULT_CF_NAME1);

	/**
	 * The 2nd column family name of the default HBase table for exercise
	 * (String)
	 */
	public static final String DEFAULT_CF_NAME2 = "CF2";

	/**
	 * The 2nd column family name of the default HBase table for exercise (byte
	 * array)
	 */
	public static final byte[] DEFAULT_CF_BIN2 = Bytes.toBytes(DEFAULT_CF_NAME2);

	/**
	 * Logger
	 */
	private static final Logger LOG = Logger.getAnonymousLogger();

	/**
	 * Constructor
	 */
	private ExerciseUtil() {
	}

	/**
	 * @return The configuration of default local HBase instance.
	 */
	public static Configuration getDefaultHBaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(getEnv("HBASE_CONF_DIR"), "hbase-site.xml"));
		conf.addResource(new Path(getEnv("HADOOP_CONF_DIR"), "core-site.xml"));
		return conf;
	}

	/**
	 * Get the value of the environment variable.
	 * 
	 * @param name
	 *            name of the variable
	 * @return value of the variable
	 */
	public static String getEnv(String name) {
		return System.getenv(name);
	}

	/**
	 * Reset the default HBase table for exercise.
	 * 
	 * @param conn
	 *            The HBase connection.
	 */
	public static void resetDefaultExerciseTable(Connection conn) {
		LOG.info("Reset the default HBase table for exercise.");
		Admin admin = null;
		try {
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

	/**
	 * @return The connection to the default HBase instance.
	 */
	public static Connection connectHBaseDefault() {
		return connectHBase(getDefaultHBaseConfiguration());
	}

	/**
	 * Connect to HBase
	 * 
	 * @param conf
	 *            HBase configuration
	 * @return The connection to HBase
	 */
	public static Connection connectHBase(Configuration conf) {
		try {
			return ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			throw new RuntimeException("Couldn't connect to HBase", e);
		}
	}

}
