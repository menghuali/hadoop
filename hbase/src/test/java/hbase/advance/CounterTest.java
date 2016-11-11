package hbase.advance;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import hbase.BaseTest;

public class CounterTest extends BaseTest {

	@Before
	public void setUp() throws Exception {
		resetDefaultTable();
	}

	@Test
	public void testSinlgeCounter() throws IOException {
		Table table = null;
		try {
			table = conn.getTable(DEFAULT_TABLE_NAME);
			byte[] row = Bytes.toBytes("row1");
			byte[] col = Bytes.toBytes("col");
			long cnt = table.incrementColumnValue(row, DEFAULT_CF_BIN1, col, 1);
			assertEquals(1, cnt);

			cnt = table.incrementColumnValue(row, DEFAULT_CF_BIN1, col, 10);
			assertEquals(11, cnt);

			cnt = table.incrementColumnValue(row, DEFAULT_CF_BIN1, col, 0);
			assertEquals(11, cnt);

			cnt = table.incrementColumnValue(row, DEFAULT_CF_BIN1, col, -1);
			assertEquals(10, cnt);
		} finally {
			if (table != null)
				table.close();
		}
	}

	@Test
	public void testMultiCounters() throws IOException {
		Table table = null;
		try {
			byte[] row = Bytes.toBytes("row1");
			Increment inc = new Increment(row);

			byte[] col1 = Bytes.toBytes("col1");
			byte[] col2 = Bytes.toBytes("col2");
			inc.addColumn(DEFAULT_CF_BIN1, col1, 1);
			inc.addColumn(DEFAULT_CF_BIN1, col2, 1);

			table = conn.getTable(DEFAULT_TABLE_NAME);
			Result rs = table.increment(inc);
			assertEquals(1, Bytes.toLong(rs.getValue(DEFAULT_CF_BIN1, col1)));
			assertEquals(1, Bytes.toLong(rs.getValue(DEFAULT_CF_BIN1, col2)));
			
			inc = new Increment(row);
			inc.addColumn(DEFAULT_CF_BIN1, col1, 1);
			inc.addColumn(DEFAULT_CF_BIN1, col2, 2);
			
			rs = table.increment(inc);
			assertEquals(2, Bytes.toLong(rs.getValue(DEFAULT_CF_BIN1, col1)));
			assertEquals(3, Bytes.toLong(rs.getValue(DEFAULT_CF_BIN1, col2)));
		} finally {
			if (table != null)
				table.close();
		}

	}

}
