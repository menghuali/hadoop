package hadoop.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

final class TableConstant {

	static class YellowPage {
		static final TableName NAME = TableName.valueOf("ypage");
		static final byte[] FAMILY = Bytes.toBytes("cf");
		static final byte[] COL_NAME = Bytes.toBytes("name");
		static final byte[] COL_ADDR = Bytes.toBytes("addr");
	}

	static class Stats {
		static final TableName NAME = TableName.valueOf("stats");
		static final byte[] FAMILY = Bytes.toBytes("cf");
		static final byte[] COL_NUM = Bytes.toBytes("num");
	}

}
