/**
 * Utility class for HBase exercise
 */
package hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author mli
 *
 */
public final class ExerciseUtil {

	private ExerciseUtil() {
	}

	public static Configuration getDefaultHBaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(getEnv("HBASE_CONF_DIR"), "hbase-site.xml"));
		conf.addResource(new Path(getEnv("HADOOP_CONF_DIR"), "core-site.xml"));
		return conf;
	}

	public static String getEnv(String name) {
		return System.getenv(name);
	}

}
