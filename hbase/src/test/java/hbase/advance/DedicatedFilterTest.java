package hbase.advance;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import hbase.BaseTest;

public class DedicatedFilterTest extends BaseTest {

	@Before
	public void setUp() throws Exception {
		resetDefaultTable();
	}

	@Test
	public void testSingleColumnValueFilter() throws IOException {
		Table table = null;
		ResultScanner rs = null;
		try {
			table = conn.getTable(DEFAULT_TABLE_NAME);
			byte[] col1 = Bytes.toBytes("col1");
			byte[] col2 = Bytes.toBytes("col2");
			for (int i = 0; i < 10; i++) {
				Put put = new Put(Bytes.toBytes(String.format("row%03d", i)));
				put.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes(String.format("val_col1_%03d", i)));
				put.addColumn(DEFAULT_CF_BIN1, col2, Bytes.toBytes(String.format("val_col2_%03d", i)));
				table.put(put);
			}

			Scan scan = new Scan();
			SingleColumnValueFilter filter = new SingleColumnValueFilter(DEFAULT_CF_BIN1, col2, CompareOp.EQUAL,
					new RegexStringComparator("val_col2_00[2468]"));
			filter.setFilterIfMissing(true);
			scan.setFilter(filter);
			rs = table.getScanner(scan);
			Result r = null;
			int index = 2;
			int size = 0;
			while ((r = rs.next()) != null) {
				assertEquals(String.format("row%03d", index), Bytes.toString(r.getRow()));
				index += 2;
				size++;
			}
			assertEquals(4, size);
		} finally {
			if (rs != null)
				rs.close();
			if (table != null)
				table.close();
		}
	}

	@Test
	public void testPrefixFilter() throws IOException {
		Table table = null;
		ResultScanner rs = null;
		try {
			table = conn.getTable(DEFAULT_TABLE_NAME);
			byte[] col = Bytes.toBytes("col");
			for (int i = 0; i < 10; i++) {
				Put put = new Put(Bytes.toBytes(String.format("row%03d", i)));
				put.addColumn(DEFAULT_CF_BIN1, col, Bytes.toBytes(String.format("val%03d", i)));
				table.put(put);
			}
			for (int i = 10; i < 20; i++) {
				Put put = new Put(Bytes.toBytes(String.format("row%03d", i)));
				put.addColumn(DEFAULT_CF_BIN1, col, Bytes.toBytes(String.format("val%03d", i)));
				table.put(put);
			}
			Scan scan = new Scan();
			scan.setFilter(new PrefixFilter(Bytes.toBytes("row01")));
			rs = table.getScanner(scan);
			int size = 0;
			Result r = null;
			while ((r = rs.next()) != null) {
				assertEquals(String.format("row01%01d", size++), Bytes.toString(r.getRow()));
			}
			assertEquals(10, size);
		} finally {
			if (table != null)
				table.close();
			if (rs != null)
				rs.close();
		}
	}

}
