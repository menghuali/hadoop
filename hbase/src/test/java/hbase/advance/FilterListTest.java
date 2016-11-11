package hbase.advance;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import hbase.BaseTest;

public class FilterListTest extends BaseTest {

	@Before
	public void setUp() throws Exception {
		resetDefaultTable();
	}

	@Test
	public void test1() throws IOException {
		Table table = null;
		ResultScanner rs = null;

		try {
			byte[] col1 = Bytes.toBytes("col1");
			byte[] col2 = Bytes.toBytes("col2");
			byte[] col3 = Bytes.toBytes("col3");

			table = conn.getTable(DEFAULT_TABLE_NAME);

			Put put = new Put(Bytes.toBytes("row1"));
			put.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes(1));
			put.addColumn(DEFAULT_CF_BIN1, col2, Bytes.toBytes(2));
			put.addColumn(DEFAULT_CF_BIN1, col3, Bytes.toBytes(3));
			table.put(put);

			put = new Put(Bytes.toBytes("row2"));
			put.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes(4));
			put.addColumn(DEFAULT_CF_BIN1, col2, Bytes.toBytes(5));
			put.addColumn(DEFAULT_CF_BIN1, col3, Bytes.toBytes(6));
			table.put(put);

			put = new Put(Bytes.toBytes("row3"));
			put.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes(7));
			put.addColumn(DEFAULT_CF_BIN1, col2, Bytes.toBytes(8));
			put.addColumn(DEFAULT_CF_BIN1, col3, Bytes.toBytes(9));
			table.put(put);

			put = new Put(Bytes.toBytes("row4"));
			put.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes(10));
			put.addColumn(DEFAULT_CF_BIN1, col2, Bytes.toBytes(11));
			put.addColumn(DEFAULT_CF_BIN1, col3, Bytes.toBytes(12));
			table.put(put);

			FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			fl.addFilter(new SkipFilter(new ValueFilter(CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(0)))));
			fl.addFilter(new SkipFilter(new ValueFilter(CompareOp.LESS, new BinaryComparator(Bytes.toBytes(10)))));
			Scan scan = new Scan();
			scan.setFilter(fl);
			rs = table.getScanner(scan);

			int size = 0;
			Result r = null;

			while ((r = rs.next()) != null) {
				size++;
				assertEquals("row" + size, Bytes.toString(r.getRow()));
			}
			assertEquals(3, size);
		} finally {
			if (rs != null)
				rs.close();
			if (table != null)
				table.close();
		}
	}

}
