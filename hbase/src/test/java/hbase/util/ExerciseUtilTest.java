package hbase.util;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ExerciseUtilTest {

	@Test
	public void testGetEnv() {
		assertEquals("/opt/hadoop-2.7.1/etc/hadoop", ExerciseUtil.getEnv("HADOOP_CONF_DIR"));
		assertEquals("/opt/hbase-1.2.2/conf", ExerciseUtil.getEnv("HBASE_CONF_DIR"));
	}

	@Test
	public void getDefaultHBaseConfiguration() {
		Configuration conf = ExerciseUtil.getDefaultHBaseConfiguration();
		assertEquals("hdfs://localhost:9000", conf.get("fs.defaultFS"));
		assertEquals("hdfs://localhost:9000/hbase", conf.get("hbase.rootdir"));
	}

}
