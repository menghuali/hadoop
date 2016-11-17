package hbase.advance;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class CoprocessorTest {

	private static final TableName TNAME = TableName.valueOf(Bytes.toBytes("mytable"));

	@Test
	public void test() throws Exception {
		HBaseTestingUtility util = new HBaseTestingUtility();
		util.startMiniCluster();
		Connection conn = util.getConnection();
		createTable(conn);

		Table table = conn.getTable(TNAME);
		byte[] row = Bytes.toBytes("row");
		Put put = new Put(row);
		put.addColumn(MyRegionObserver.CF, MyRegionObserver.Q_SRC, Bytes.toBytes("value"));
		table.put(put);

		Get get = new Get(row);
		Result r = table.get(get);

		assertEquals("value", Bytes.toString(r.getValue(MyRegionObserver.CF, MyRegionObserver.Q_CLONE)));
	}

	private void createTable(Connection conn) throws IOException {
		Admin adm = null;
		try {
			adm = conn.getAdmin();
			if (adm.tableExists(TNAME)) {
				adm.disableTable(TNAME);
				adm.deleteTable(TNAME);
			}
			HTableDescriptor tdes = new HTableDescriptor(TNAME);
			tdes.addFamily(new HColumnDescriptor(MyRegionObserver.CF));
			// define coprocessor
			tdes.setValue("COPROCESSOR$1",
					"|" + MyRegionObserver.class.getCanonicalName() + "|" + Coprocessor.PRIORITY_USER);
			adm.createTable(tdes);
		} finally {
			adm.close();
		}
	}

}
