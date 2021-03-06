package hbase.base;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import hbase.BaseTest;

public class DeleteTest extends BaseTest {
	private byte[] row = generateRowKeyBytes(DeleteTest.class, "row1");
	private byte[] col1 = Bytes.toBytes("col1");
	private byte[] col2 = Bytes.toBytes("col2");
	private Table table;
	private long timestamp1;
	private long timestamp2;
	private long timestamp3;
	private long timestamp4;

	@Before
	public void setUp() throws Exception {
		super.setUp();
		table = conn.getTable(DEFAULT_TABLE_NAME);
		timestamp1 = System.currentTimeMillis() - 5000;
		timestamp2 = timestamp1 + 1000;
		timestamp3 = timestamp1 + 2000;
		timestamp4 = timestamp1 + 3000;

		Put put1 = new Put(row);
		put1.addColumn(DEFAULT_CF_BIN1, col1, timestamp1, Bytes.toBytes("cf1_col1_v1"));
		table.put(put1);

		Put put2 = new Put(row);
		put2.addColumn(DEFAULT_CF_BIN1, col1, timestamp2, Bytes.toBytes("cf1_col1_v2"));
		table.put(put2);

		Put put3 = new Put(row);
		put3.addColumn(DEFAULT_CF_BIN1, col2, timestamp3, Bytes.toBytes("cf1_col2_v1"));
		table.put(put3);

		Put put4 = new Put(row);
		put4.addColumn(DEFAULT_CF_BIN2, col1, timestamp4, Bytes.toBytes("cf2_col1_v1"));
		table.put(put4);
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
		table.close();
		table = null;
		BaseTest.resetDefaultTable();
	}

	@Test
	public void testDeleteEntireRow() throws Exception {
		Delete del = new Delete(row);
		table.delete(del);

		Get get = new Get(row);
		Result result = table.get(get);
		assertNull("The row of the result should be null because the entire row was deleted.", result.getRow());
	}

	@Test
	public void testCheckAndDeleteNotNull() throws IOException {
		Delete del = new Delete(row);
		assertFalse("Thr row shouldn't be deleted because cf1:col1 exists",
				table.checkAndDelete(row, DEFAULT_CF_BIN1, col1, null, del));
	}

	@Test
	public void testCheckAndDeleteNull() throws IOException {
		Delete del = new Delete(row);
		assertTrue("The row should have been deleted because cf1:col3 does not exists.",
				table.checkAndDelete(row, DEFAULT_CF_BIN1, Bytes.toBytes("col3"), null, del));
	}

	@Test
	public void testCheckAndDeleteFail() throws IOException {
		Delete del = new Delete(row);
		assertFalse("The row shouldn't be deleted because it checks the over version of the cell",
				table.checkAndDelete(row, DEFAULT_CF_BIN1, col1, Bytes.toBytes("cf1_col1_v1"), del));
		assertNotNull("The row shouldn't be deleted.", table.get(new Get(row)).getRow());
	}

	@Test
	public void testCheckAndDelete() throws IOException {
		Delete del = new Delete(row);
		assertTrue("The row should have been deleted",
				table.checkAndDelete(row, DEFAULT_CF_BIN1, col1, Bytes.toBytes("cf1_col1_v2"), del));
		assertNull("The row should have been deleted.", table.get(new Get(row)).getRow());
	}

	@Test
	public void testListOfDeletes() throws IOException {
		byte[] row1 = generateRowKeyBytes(DeleteTest.class, "testListOfDeletes#1");
		byte[] row2 = generateRowKeyBytes(DeleteTest.class, "testListOfDeletes#2");
		Put put1 = new Put(row1);
		put1.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes("val1"));
		Put put2 = new Put(row2);
		put2.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes("val2"));
		table.put(Arrays.asList(new Put[] { put1, put2 }));

		Delete del1 = new Delete(row1);
		Delete del2 = new Delete(row2);
		ArrayList<Delete> dels = new ArrayList<>();
		dels.add(del1);
		dels.add(del2);
		table.delete(dels);

		Result[] result = table.get(Arrays.asList(new Get[] { new Get(row1), new Get(row2) }));
		assertEquals(2, result.length);
		assertNull(result[0].getRow());
		assertNull(result[1].getRow());
	}

	@Test
	public void testListOfDeletesWithFailure() throws IOException {
		byte[] row1 = generateRowKeyBytes(DeleteTest.class, "testListOfDeletes#1");
		byte[] row2 = generateRowKeyBytes(DeleteTest.class, "testListOfDeletes#2");
		Put put1 = new Put(row1);
		put1.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes("val1"));
		Put put2 = new Put(row2);
		put2.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes("val2"));
		table.put(Arrays.asList(new Put[] { put1, put2 }));

		Delete del1 = new Delete(row1);
		Delete del2 = new Delete(row2);
		// del2 causes failure because of family not exist.
		del2.addFamily(Bytes.toBytes("BOGUS"));
		// del3 still delete row2
		Delete del3 = new Delete(row2);
		ArrayList<Delete> dels = new ArrayList<>();
		dels.add(del1);
		dels.add(del2);
		dels.add(del3);
		try {
			table.delete(dels);
		} catch (Exception e) {
			e.printStackTrace();
		}

		assertEquals(1, dels.size());

		Result[] result = table.get(Arrays.asList(new Get[] { new Get(row1), new Get(row2) }));
		assertEquals(2, result.length);
		assertNull("Row1 should have been deleted by del1", result[0].getRow());
		assertNull("Row2 should have been deleted by del3", result[1].getRow());
	}

	@Test
	public void testDeleteEntireRowSpecialVersion() throws Exception {
		Delete del = new Delete(row);
		del.setTimestamp(timestamp1 + 100);
		table.delete(del);

		Get get = new Get(row);
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		assertEquals("There should be 3 cells found", 3, cells.size());
		HashSet<String> values = new HashSet<>();
		for (Cell cell : cells) {
			values.add(Bytes.toString(CellUtil.cloneValue(cell)));
		}
		assertTrue("The get should return the latest version of cf1:col1", values.contains("cf1_col1_v2"));
		assertTrue("The get should return the latest version of cf1:col2", values.contains("cf1_col2_v1"));
		assertTrue("The get should return the latest version of cf2:col1", values.contains("cf2_col1_v1"));
	}

	@Test
	public void testDeleteColumn() throws IOException {
		Delete del = new Delete(row);
		del.addColumn(DEFAULT_CF_BIN1, col1);
		table.delete(del);

		Get get = new Get(row);
		get.setMaxVersions();
		get.addColumn(DEFAULT_CF_BIN1, col1);
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		assertEquals(1, cells.size());
		assertEquals("The older version should exist only the latest version was deleted", "cf1_col1_v1",
				Bytes.toString(CellUtil.cloneValue(cells.get(0))));
	}

	@Test
	public void testDeleteColumnSpecialVersion() throws IOException {
		Delete del = new Delete(row);
		del.addColumn(DEFAULT_CF_BIN1, col1, timestamp1);
		table.delete(del);

		Get get = new Get(row);
		get.addColumn(DEFAULT_CF_BIN1, col1);
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		assertEquals(1, cells.size());
		assertEquals("The latest version should exist because only the old version was deleted.", "cf1_col1_v2",
				Bytes.toString(CellUtil.cloneValue(cells.get(0))));
	}

	@Test
	public void testDeleteColumns() throws IOException {
		Delete del = new Delete(row);
		del.addColumns(DEFAULT_CF_BIN1, col1);
		table.delete(del);

		Get get = new Get(row);
		get.addColumn(DEFAULT_CF_BIN1, col1);
		assertNull("All versions in that column should have been removed.", table.get(get).getRow());
	}

	@Test
	public void testDeleteColumnsSpecialVersion() throws IOException {
		Delete del = new Delete(row);
		del.addColumns(DEFAULT_CF_BIN1, col1, timestamp1 + 100);
		table.delete(del);

		Get get = new Get(row);
		get.addColumn(DEFAULT_CF_BIN1, col1);
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		assertEquals(1, cells.size());
		assertEquals("The lastest version of the column should exists because only the old version was deleted",
				"cf1_col1_v2", Bytes.toString(CellUtil.cloneValue(cells.get(0))));
	}

	@Test
	public void testDeleteFamily() throws IOException {
		Delete del = new Delete(row);
		del.addFamily(DEFAULT_CF_BIN1);
		table.delete(del);

		Get get = new Get(row);
		get.addFamily(DEFAULT_CF_BIN1);
		Result result = table.get(get);
		assertNull("All family values should have been deleted.", result.getRow());

		get = new Get(row);
		get.addFamily(DEFAULT_CF_BIN2);
		assertNotNull("Family 2 should exists", table.get(get).getRow());
	}

	@Test
	public void testDeleteFamilySpecialVersion() throws IOException {
		Delete del = new Delete(row);
		del.addFamily(DEFAULT_CF_BIN1, timestamp1 + 100);
		table.delete(del);

		Get get = new Get(row);
		get.addFamily(DEFAULT_CF_BIN1);
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		assertEquals("There should be 2 cells found", 2, cells.size());
		HashSet<String> values = new HashSet<>();
		for (Cell cell : cells) {
			values.add(Bytes.toString(CellUtil.cloneValue(cell)));
		}
		assertTrue("The get should return the latest version of cf1:col1", values.contains("cf1_col1_v2"));
		assertTrue("The get should return the latest version of cf1:col2", values.contains("cf1_col2_v1"));
	}

}
