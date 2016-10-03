/**
 * 
 */
package hbase.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import hbase.BaseTest;

/**
 * @author mli
 *
 */
public class GetTest extends BaseTest {
	private static byte[] row = generateRowKeyBytes(GetTest.class, "row1");
	private static byte[] col = Bytes.toBytes("col");
	private static long timestamp1 = System.currentTimeMillis();
	private static long timestamp2 = timestamp1 + 1000;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BaseTest.setUpBeforeClass();

		Put put1 = new Put(row);
		put1.addColumn(DEFAULT_CF_BIN1, col, timestamp1, Bytes.toBytes("val1"));
		put1.addColumn(DEFAULT_CF_BIN2, col, timestamp1, Bytes.toBytes("vala"));
		Put put2 = new Put(row);
		put2.addColumn(DEFAULT_CF_BIN1, col, timestamp2, Bytes.toBytes("val2"));
		put2.addColumn(DEFAULT_CF_BIN2, col, timestamp2, Bytes.toBytes("valb"));
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		table.put(put1);
		table.put(put2);
		table.close();
	}

	@Test
	public void testGet() throws Exception {
		Get get = new Get(row);
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		Result result = table.get(get);
		assertEquals(row, get.getRow());
		assertTrue(result.containsColumn(DEFAULT_CF_BIN1, col));
		assertEquals("val2", Bytes.toString(result.getValue(DEFAULT_CF_BIN1, col)));
		assertTrue(result.containsColumn(DEFAULT_CF_BIN2, col));
		assertEquals("valb", Bytes.toString(result.getValue(DEFAULT_CF_BIN2, col)));
	}

	@Test
	public void testGetMultiVersions() throws IOException {
		Get get = new Get(row);
		get.setMaxVersions();
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		Result result = table.get(get);
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();

		assertEquals(2, map.size());
		assertTrue(map.containsKey(DEFAULT_CF_BIN1));
		NavigableMap<byte[], NavigableMap<Long, byte[]>> cf1 = map.get(DEFAULT_CF_BIN1);
		assertEquals(1, cf1.size());
		NavigableMap<Long, byte[]> cell1 = cf1.get(col);
		assertEquals(2, cell1.size());
		assertEquals("val1", Bytes.toString(cell1.get(timestamp1)));
		assertEquals("val2", Bytes.toString(cell1.get(timestamp2)));

		assertTrue(map.containsKey(DEFAULT_CF_BIN2));
		NavigableMap<byte[], NavigableMap<Long, byte[]>> cf2 = map.get(DEFAULT_CF_BIN2);
		assertEquals(1, cf2.size());
		assertTrue(cf2.containsKey(col));
		NavigableMap<Long, byte[]> cell2 = cf2.get(col);
		assertEquals(2, cell2.size());
		assertEquals("vala", Bytes.toString(cell2.get(timestamp1)));
		assertEquals("valb", Bytes.toString(cell2.get(timestamp2)));
	}

	@Test
	public void testGetTimeRange() throws IOException {
		Get get = new Get(row);
		get.setMaxVersions();
		get.setTimeRange(timestamp1 - 100, timestamp1 + 1000);
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		table.get(get);
		Result result = table.get(get);
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();

		NavigableMap<byte[], NavigableMap<Long, byte[]>> cf1 = map.get(DEFAULT_CF_BIN1);
		NavigableMap<Long, byte[]> cell1 = cf1.get(col);
		assertEquals(1, cell1.size());
		assertEquals("val1", Bytes.toString(cell1.get(timestamp1)));

		NavigableMap<byte[], NavigableMap<Long, byte[]>> cf2 = map.get(DEFAULT_CF_BIN2);
		NavigableMap<Long, byte[]> cell2 = cf2.get(col);
		assertEquals(1, cell2.size());
		assertEquals("vala", Bytes.toString(cell2.get(timestamp1)));
	}

}
