package hbase.base;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import hbase.BaseTest;

public class ComparisonFilterTest extends BaseTest {

	@Before
	public void setUp() throws Exception {
		resetDefaultTable();
	}

	@Test
	public void testRowFilter_Less() throws IOException {
		Table table = null;
		try {
			String rowPrefix = generateRowKey(ComparisonFilterTest.class, "testRowFilter");
			table = putRows(10, rowPrefix);

			Filter less = new RowFilter(CompareOp.LESS, new BinaryComparator(Bytes.toBytes(rowPrefix + "_row005")));
			List<Result> result = scan(table, less);
			assertEquals(5, result.size());
			assertEquals(rowPrefix + "_row000", Bytes.toString(result.get(0).getRow()));
			assertEquals(rowPrefix + "_row001", Bytes.toString(result.get(1).getRow()));
			assertEquals(rowPrefix + "_row002", Bytes.toString(result.get(2).getRow()));
			assertEquals(rowPrefix + "_row003", Bytes.toString(result.get(3).getRow()));
			assertEquals(rowPrefix + "_row004", Bytes.toString(result.get(4).getRow()));
		} finally {
			if (table != null)
				table.close();
		}
	}

	@Test
	public void testRowFilter_LessOrEqual() throws IOException {
		Table table = null;
		try {
			String rowPrefix = generateRowKey(ComparisonFilterTest.class, "testRowFilter");
			table = putRows(10, rowPrefix);

			Filter lessEqual = new RowFilter(CompareOp.LESS_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(rowPrefix + "_row005")));
			List<Result> result = scan(table, lessEqual);
			assertEquals(6, result.size());
			assertEquals(rowPrefix + "_row000", Bytes.toString(result.get(0).getRow()));
			assertEquals(rowPrefix + "_row001", Bytes.toString(result.get(1).getRow()));
			assertEquals(rowPrefix + "_row002", Bytes.toString(result.get(2).getRow()));
			assertEquals(rowPrefix + "_row003", Bytes.toString(result.get(3).getRow()));
			assertEquals(rowPrefix + "_row004", Bytes.toString(result.get(4).getRow()));
			assertEquals(rowPrefix + "_row005", Bytes.toString(result.get(5).getRow()));
		} finally {
			if (table != null)
				table.close();
		}
	}

	@Test
	public void testRowFilter_Less_WithStartEndRows() throws IOException {
		Table table = null;
		try {
			String rowPrefix = generateRowKey(ComparisonFilterTest.class, "testRowFilter");
			table = putRows(10, rowPrefix);

			Filter lessEqual = new RowFilter(CompareOp.LESS_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(rowPrefix + "_row005")));
			Scan scan = new Scan();
			scan.setFilter(lessEqual);
			scan.setStartRow(Bytes.toBytes(rowPrefix + "_row001"));
			scan.setStopRow(Bytes.toBytes(rowPrefix + "_row005"));

			List<Result> result = scan(table, scan);
			assertEquals(4, result.size());
			assertEquals(rowPrefix + "_row001", Bytes.toString(result.get(0).getRow()));
			assertEquals(rowPrefix + "_row002", Bytes.toString(result.get(1).getRow()));
			assertEquals(rowPrefix + "_row003", Bytes.toString(result.get(2).getRow()));
			assertEquals(rowPrefix + "_row004", Bytes.toString(result.get(3).getRow()));
		} finally {
			if (table != null)
				table.close();
		}
	}

	private Table putRows(int size, String rowPrefix) throws IOException {
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		if (size <= 0 || rowPrefix == null || rowPrefix.trim().length() == 0)
			return table;
		ArrayList<Put> put = new ArrayList<>();
		byte[] col = Bytes.toBytes("col");
		for (int i = 0; i < size; i++) {
			Put p = new Put(Bytes.toBytes(rowPrefix + String.format("_row%03d", i)));
			put.add(p);
			p.addColumn(DEFAULT_CF_BIN1, col, Bytes.toBytes(String.format("val%03d", i)));
		}
		table.put(put);
		return table;
	}

	private List<Result> scan(Table table, Filter filter) throws IOException {
		Scan scan = new Scan();
		scan.setFilter(filter);
		return scan(table, scan);
	}

	private List<Result> scan(Table table, Scan scan) throws IOException {
		ResultScanner rs = null;
		try {
			rs = table.getScanner(scan);
			ArrayList<Result> result = new ArrayList<>();
			Result r = null;
			while ((r = rs.next()) != null)
				result.add(r);
			return result;
		} finally {
			rs.close();
		}
	}

}
