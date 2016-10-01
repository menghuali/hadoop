package hbase.base;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import hbase.BaseTest;

public class PutTest extends BaseTest {

	/**
	 * Test single put
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSinlgePut() throws Exception {
		byte[] row = Bytes.toBytes(generateRowKey(PutTest.class, "testSinlgePut"));
		byte[] col1 = Bytes.toBytes("col1");
		byte[] col2 = Bytes.toBytes("col2");

		Put put = new Put(row);
		assertTrue("Put should be empty", put.isEmpty());

		put.addColumn(DEFAULT_CF_BIN1, col1, Bytes.toBytes(100));
		put.addColumn(DEFAULT_CF_BIN1, col2, Bytes.toBytes("abc"));
		assertFalse("Put shouldn't be empty", put.isEmpty());
		assertEquals("Family size should be 1", 1, put.numFamilies());
		assertEquals("KeyValue size should be 2", 2, put.size());
		assertEquals("Timestamp should be max long", Long.MAX_VALUE, put.getTimeStamp());
		assertTrue("ColumnFamily1 should be found", put.has(DEFAULT_CF_BIN1, col1));
		assertTrue("ColumnFamily1:column2 should be found", put.has(DEFAULT_CF_BIN1, col2, Bytes.toBytes("abc")));

		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		table.put(put);

		Get get = new Get(row);
		Result result = table.get(get);

		assertNotNull("1 result should be found", result.getRow());
		assertEquals("RowKey should be expected value", Bytes.toString(row), Bytes.toString(result.getRow()));

		byte[] val = result.getValue(DEFAULT_CF_BIN1, col1);
		assertEquals("CF1:col1 should be expected value", 100, Bytes.toInt(val));

		val = result.getValue(DEFAULT_CF_BIN1, col2);
		assertEquals("CF1:col2 should be expected value", "abc", Bytes.toString(val));
	}

	/**
	 * Test single put with KeyValue
	 * 
	 * @throws Exception
	 */
	@Test
	public void testKeyValue() throws Exception {
		byte[] row = Bytes.toBytes(generateRowKey(PutTest.class, "testKeyValue"));
		byte[] qualifier = Bytes.toBytes("col1");
		byte[] value = Bytes.toBytes("abc");

		// Create KeyValue and add to put
		KeyValue kv = new KeyValue(row, DEFAULT_CF_BIN1, qualifier, value);
		Put put = new Put(row);
		put.add(kv); // End

		assertFalse("Put shouldn't be empty", put.isEmpty());
		assertEquals("Family size should be 1", 1, put.numFamilies());
		assertEquals("KeyValue size should be 1", 1, put.size());
		assertEquals("Timestamp should be max long", Long.MAX_VALUE, put.getTimeStamp());
		assertTrue("ColumnFamily1 should be found", put.has(DEFAULT_CF_BIN1, qualifier));
		assertTrue("ColumnFamily1:column1 should be found", put.has(DEFAULT_CF_BIN1, qualifier, value));

		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		table.put(put);

		Get get = new Get(row);
		Result result = table.get(get);

		assertNotNull("1 result should be found", result.getRow());
		assertEquals("RowKey should be expected value", Bytes.toString(row), Bytes.toString(result.getRow()));

		byte[] val = result.getValue(DEFAULT_CF_BIN1, qualifier);
		assertEquals("CF1:col1 should be expected value", "abc", Bytes.toString(val));
	}

	/**
	 * Test client side buffer
	 * 
	 * @throws Exception
	 */
	@Test
	public void testClientSideBuffer() throws Exception {
		byte[] qualifier = Bytes.toBytes("col");
		BufferedMutator buff = conn.getBufferedMutator(DEFAULT_TABLE_NAME);

		byte[] row1 = Bytes.toBytes(generateRowKey(PutTest.class, "testClientSideBufferRow1"));
		Put put1 = new Put(row1);
		put1.addColumn(DEFAULT_CF_BIN1, qualifier, Bytes.toBytes("value1"));
		buff.mutate(put1);

		byte[] row2 = Bytes.toBytes(generateRowKey(PutTest.class, "testClientSideBufferRow2"));
		Put put2 = new Put(row2);
		put2.addColumn(DEFAULT_CF_BIN1, qualifier, Bytes.toBytes("value2"));
		buff.mutate(put2);

		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		Get get1 = new Get(row1);
		Result result1 = table.get(get1);
		assertNull("Result1 rowkey should be null because buffer hasn't been flushed", result1.getRow());

		Get get2 = new Get(row2);
		Result result2 = table.get(get2);
		assertNull("Result2 rowkey should be null because buffer hasn't been flushed", result2.getRow());

		// Flush client side buffered changes: put1 and put2
		buff.flush();

		result1 = table.get(get1);
		assertNotNull("Result1 shouldn't be null because buffer has been flushed", result1);
		assertEquals("RowKey should be expected value", Bytes.toString(row1), Bytes.toString(result1.getRow()));
		byte[] value1 = result1.getValue(DEFAULT_CF_BIN1, qualifier);
		assertEquals("CF1:col should be expected value", "value1", Bytes.toString(value1));

		result2 = table.get(get2);
		assertNotNull("Result2 shouldn't be null because buffer has been flushed", result2);
		assertEquals("RowKey should be expected value", Bytes.toString(row2), Bytes.toString(result2.getRow()));
		byte[] value2 = result2.getValue(DEFAULT_CF_BIN1, qualifier);
		assertEquals("CF1:col should be expected value", "value2", Bytes.toString(value2));
	}

	/**
	 * Test list of puts
	 * 
	 * @throws Exception
	 */
	@Test
	public void testListOfPuts() throws Exception {
		byte[] qualifier = Bytes.toBytes("col");
		byte[] row1 = Bytes.toBytes(generateRowKey(PutTest.class, "testListOfPutsRow1"));
		Put put1 = new Put(row1);
		put1.addColumn(DEFAULT_CF_BIN1, qualifier, Bytes.toBytes("value1"));

		byte[] row2 = Bytes.toBytes(generateRowKey(PutTest.class, "testListOfPutsRow2"));
		Put put2 = new Put(row2);
		put2.addColumn(DEFAULT_CF_BIN1, qualifier, Bytes.toBytes("value2"));

		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		table.put(Arrays.asList(new Put[] { put1, put2 }));

		Get get1 = new Get(row1);
		Result result1 = table.get(get1);
		assertNotNull("Result1 shouldn't be null", result1.getRow());
		assertEquals("RowKey should be expected value", Bytes.toString(row1), Bytes.toString(result1.getRow()));
		byte[] value1 = result1.getValue(DEFAULT_CF_BIN1, qualifier);
		assertEquals("CF1:col should be expected value1", "value1", Bytes.toString(value1));

		Get get2 = new Get(row2);
		Result result2 = table.get(get2);
		assertNotNull("Result2 shouldn't be null", result2.getRow());
		assertEquals("RowKey should be expected value", Bytes.toString(row2), Bytes.toString(result2.getRow()));
		byte[] value2 = result2.getValue(DEFAULT_CF_BIN1, qualifier);
		assertEquals("CF1:col should be expected value", "value2", Bytes.toString(value2));
	}

	/**
	 * Test checkAndPut
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCheckAndPut() throws Exception {
		byte[] row = Bytes.toBytes(generateRowKey(PutTest.class, "testCheckAndPut"));
		byte[] qualifier = Bytes.toBytes("col");
		byte[] value1 = Bytes.toBytes("value1");
		Put put1 = new Put(row);
		put1.addColumn(DEFAULT_CF_BIN1, qualifier, value1);

		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		table.put(put1);

		Get get = new Get(row);
		Result result = table.get(get);
		byte[] val = result.getValue(DEFAULT_CF_BIN1, qualifier);
		assertEquals("CF1:col should be expected value", "value1", Bytes.toString(val));

		Put put2 = new Put(row);
		byte[] value2 = Bytes.toBytes("value2");
		put2.addColumn(DEFAULT_CF_BIN1, qualifier, value2);
		assertTrue(table.checkAndPut(row, DEFAULT_CF_BIN1, qualifier, value1, put2));
		result = table.get(get);
		val = result.getValue(DEFAULT_CF_BIN1, qualifier);
		assertEquals("CF1:col should be expected value", "value2", Bytes.toString(val));

		Put put3 = new Put(row);
		put3.addColumn(DEFAULT_CF_BIN1, qualifier, Bytes.toBytes("value3"));
		assertFalse(table.checkAndPut(row, DEFAULT_CF_BIN1, qualifier, value1, put3));
		result = table.get(get);
		val = result.getValue(DEFAULT_CF_BIN1, qualifier);
		assertEquals("CF1:col should still be value2 checkAndPut fails", "value2", Bytes.toString(val));
	}

}
