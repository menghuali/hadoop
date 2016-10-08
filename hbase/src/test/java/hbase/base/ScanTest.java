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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import hbase.BaseTest;

public class ScanTest extends BaseTest {

	private static List<byte[]> columns;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BaseTest.setUpBeforeClass();
		setupColumns();
		ArrayList<Put> puts = new ArrayList<>(100);
		for (int i = 0; i < 100; i++) {
			puts.add(addRow(i));
		}
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		table.put(puts);
		table.close();
	}

	public static void setupColumns() {
		columns = new ArrayList<>();
		for (int i = 0; i < 10; i++)
			columns.add(Bytes.toBytes("col" + i));
	}

	public static Put addRow(int rowNum) {
		String prefix = rowNum < 10 ? "#row0" : "#row";
		Put put = new Put(generateRowKeyBytes(ScanTest.class, prefix + rowNum));
		for (byte[] col : columns) {
			put.addColumn(DEFAULT_CF_BIN1, col, Bytes.toBytes("val" + rowNum));
		}
		return put;
	}

	@Test
	public void testScan() throws IOException {
		ResultScanner rs = null;
		try {
			Scan scan = new Scan();
			scan.setStartRow(generateRowKeyBytes(ScanTest.class, "#row" + 10));
			scan.setStopRow(generateRowKeyBytes(ScanTest.class, "#row" + 20));
			Table table = conn.getTable(DEFAULT_TABLE_NAME);
			rs = table.getScanner(scan);
			Result result = null;
			int size = 0;
			while ((result = rs.next()) != null) {
				assertFalse(result.isEmpty());
				assertEquals(columns.size(), result.size());
				size++;
			}
			assertEquals(10, size);
		} finally {
			if (rs != null)
				rs.close();
		}
	}

	@Test
	public void testScanColumns() throws IOException {
		ResultScanner rs = null;
		Scan scan = new Scan(generateRowKeyBytes(ScanTest.class, "#row" + 10),
				generateRowKeyBytes(ScanTest.class, "#row" + 20));
		scan.addColumn(DEFAULT_CF_BIN1, columns.get(0));
		scan.addColumn(DEFAULT_CF_BIN1, columns.get(1));
		scan.addColumn(DEFAULT_CF_BIN1, columns.get(2));
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		rs = table.getScanner(scan);
		Result result = null;
		while ((result = rs.next()) != null) {
			assertEquals(3, result.size());
		}
	}

	@Test
	public void testScanCacheAndBatch() throws IOException {
		ResultScanner rs = null;
		Scan scan = new Scan();
		scan.setCaching(10);
		scan.setBatch(5);
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		rs = table.getScanner(scan);
		Result result = null;
		int i = 0;
		String row = null;
		while ((result = rs.next()) != null) {
			assertEquals(5, result.size());
			if (i % 2 == 0) {
				row = Bytes.toString(result.getRow());
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(0)));
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(1)));
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(2)));
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(3)));
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(4)));
			}
			if (i % 2 == 1) {
				assertEquals(row, Bytes.toString(result.getRow()));
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(5)));
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(6)));
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(7)));
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(8)));
				assertTrue(result.containsColumn(DEFAULT_CF_BIN1, columns.get(9)));
			}
			i++;
		}
	}

}
