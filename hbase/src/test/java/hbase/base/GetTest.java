/**
 * Test Get
 */
package hbase.base;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import hbase.BaseTest;

/**
 * @author mli
 *
 */
public class GetTest extends BaseTest {
	private static byte[] row1 = generateRowKeyBytes(GetTest.class, "row1");
	private static byte[] qualifier = Bytes.toBytes("col");
	private static long timestamp1 = System.currentTimeMillis();
	private static long timestamp2 = timestamp1 + 1000;
	private static byte[] row2 = generateRowKeyBytes(GetTest.class, "row2");

	/**
	 * Create default rows in the default table for exercise.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BaseTest.setUpBeforeClass();

		Put put1 = new Put(row1);
		put1.addColumn(DEFAULT_CF_BIN1, qualifier, timestamp1, Bytes.toBytes("val1"));
		put1.addColumn(DEFAULT_CF_BIN2, qualifier, timestamp1, Bytes.toBytes("vala"));

		Put put2 = new Put(row1);
		put2.addColumn(DEFAULT_CF_BIN1, qualifier, timestamp2, Bytes.toBytes("val2"));
		put2.addColumn(DEFAULT_CF_BIN2, qualifier, timestamp2, Bytes.toBytes("valb"));

		Put put3 = new Put(row2);
		put3.addColumn(DEFAULT_CF_BIN1, qualifier, Bytes.toBytes("val3"));
		put3.addColumn(DEFAULT_CF_BIN2, qualifier, Bytes.toBytes("valc"));

		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		table.put(put1);
		table.put(put2);
		table.put(put3);
		table.close();
	}

	/**
	 * Test simple get
	 * 
	 * @throws Exception
	 */
	@Test
	public void testGet() throws Exception {
		Get get = new Get(row1);
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		Result result = table.get(get);
		assertEquals(row1, get.getRow());
		assertTrue(result.containsColumn(DEFAULT_CF_BIN1, qualifier));
		assertEquals("val2", Bytes.toString(result.getValue(DEFAULT_CF_BIN1, qualifier)));
		assertTrue(result.containsColumn(DEFAULT_CF_BIN2, qualifier));
		assertEquals("valb", Bytes.toString(result.getValue(DEFAULT_CF_BIN2, qualifier)));
	}

	/**
	 * Test get multiple versions
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGetMultiVersions() throws IOException {
		Get get = new Get(row1);
		get.setMaxVersions();
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		Result result = table.get(get);
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();

		assertEquals(2, map.size());
		assertTrue(map.containsKey(DEFAULT_CF_BIN1));
		NavigableMap<byte[], NavigableMap<Long, byte[]>> cf1 = map.get(DEFAULT_CF_BIN1);
		assertEquals(1, cf1.size());
		NavigableMap<Long, byte[]> cell1 = cf1.get(qualifier);
		assertEquals(2, cell1.size());
		assertEquals("val1", Bytes.toString(cell1.get(timestamp1)));
		assertEquals("val2", Bytes.toString(cell1.get(timestamp2)));

		assertTrue(map.containsKey(DEFAULT_CF_BIN2));
		NavigableMap<byte[], NavigableMap<Long, byte[]>> cf2 = map.get(DEFAULT_CF_BIN2);
		assertEquals(1, cf2.size());
		assertTrue(cf2.containsKey(qualifier));
		NavigableMap<Long, byte[]> cell2 = cf2.get(qualifier);
		assertEquals(2, cell2.size());
		assertEquals("vala", Bytes.toString(cell2.get(timestamp1)));
		assertEquals("valb", Bytes.toString(cell2.get(timestamp2)));
	}

	/**
	 * Test get with time range
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGetTimeRange() throws IOException {
		Get get = new Get(row1);
		get.setMaxVersions();
		get.setTimeRange(timestamp1 - 100, timestamp1 + 1000);
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		table.get(get);
		Result result = table.get(get);
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();

		NavigableMap<byte[], NavigableMap<Long, byte[]>> cf1 = map.get(DEFAULT_CF_BIN1);
		NavigableMap<Long, byte[]> cell1 = cf1.get(qualifier);
		assertEquals(1, cell1.size());
		assertEquals("val1", Bytes.toString(cell1.get(timestamp1)));

		NavigableMap<byte[], NavigableMap<Long, byte[]>> cf2 = map.get(DEFAULT_CF_BIN2);
		NavigableMap<Long, byte[]> cell2 = cf2.get(qualifier);
		assertEquals(1, cell2.size());
		assertEquals("vala", Bytes.toString(cell2.get(timestamp1)));
	}

	/**
	 * Test list of gets
	 * 
	 * @throws IOException
	 */
	@Test
	public void testListOfGets() throws IOException {
		Get get1 = new Get(row1);
		Get get2 = new Get(row2);
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		Result[] result = table.get(Arrays.asList(new Get[] { get1, get2 }));
		assertEquals(2, result.length);
		HashSet<String> rows = new HashSet<>();
		rows.add(Bytes.toString(result[0].getRow()));
		rows.add(Bytes.toString(result[1].getRow()));
		assertEquals(2, rows.size());
		assertTrue(rows.contains(Bytes.toString(row1)));
		assertTrue(rows.contains(Bytes.toString(row2)));
	}

	/**
	 * Test filter
	 * 
	 * @throws IOException
	 */
	@Test
	public void testFilter() throws IOException {
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		byte[] row = generateRowKeyBytes(GetTest.class, "testFilter");
		Put put = new Put(row);
		put.addColumn(DEFAULT_CF_BIN1, Bytes.toBytes("cola_1"), Bytes.toBytes("vala_1"));
		put.addColumn(DEFAULT_CF_BIN1, Bytes.toBytes("cola_2"), Bytes.toBytes("vala_2"));
		put.addColumn(DEFAULT_CF_BIN1, Bytes.toBytes("colb_1"), Bytes.toBytes("valb_2"));
		table.put(put);

		Get get = new Get(row);
		get.setFilter(new ColumnPrefixFilter(Bytes.toBytes("cola")));
		Result result = table.get(get);

		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
		NavigableMap<byte[], NavigableMap<Long, byte[]>> cf1 = map.get(DEFAULT_CF_BIN1);
		assertEquals(2, cf1.size());
		NavigableMap<Long, byte[]> cola_1 = cf1.get(Bytes.toBytes("cola_1"));
		assertEquals(1, cola_1.size());
		assertEquals("vala_1", Bytes.toString(cola_1.values().iterator().next()));
		NavigableMap<Long, byte[]> cola_2 = cf1.get(Bytes.toBytes("cola_2"));
		assertEquals(1, cola_2.size());
		assertEquals("vala_2", Bytes.toString(cola_2.values().iterator().next()));
	}

	/**
	 * Test Table.exists(Get)
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExists() throws IOException {
		byte[] row = generateRowKeyBytes(GetTest.class, "testExists1");
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		Get get = new Get(row);
		assertFalse(table.exists(get));

		Put put = new Put(row);
		put.addColumn(DEFAULT_CF_BIN1, qualifier, Bytes.toBytes("val1"));
		table.put(put);
		assertTrue(table.exists(get));
	}

}
