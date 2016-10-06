package hbase.base;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import hbase.BaseTest;

public class BatchTest extends BaseTest {

	@Test
	public void testBatch() throws IOException, InterruptedException {
		Table table = conn.getTable(DEFAULT_TABLE_NAME);
		Put put1 = new Put(generateRowKeyBytes(BatchTest.class, "row1"));
		put1.addColumn(DEFAULT_CF_BIN1, Bytes.toBytes("col1"), Bytes.toBytes("val1"));
		Put put2 = new Put(generateRowKeyBytes(BatchTest.class, "row2"));
		put2.addColumn(DEFAULT_CF_BIN1, Bytes.toBytes("col1"), Bytes.toBytes("val2"));
		Result[] results = new Result[2];
		table.batch(Arrays.asList(new Row[] { put1, put2 }), results);

		Delete del = new Delete(put1.getRow());
		Put put3 = new Put(generateRowKeyBytes(BatchTest.class, "row3"));
		put3.addColumn(DEFAULT_CF_BIN1, Bytes.toBytes("col1"), Bytes.toBytes("val3"));
		Get get = new Get(put2.getRow());
		results = new Result[3];
		try {
			table.batch(Arrays.asList(new Row[] { del, put3, get }), results);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertTrue(results[0].isEmpty());
		assertTrue(results[1].isEmpty());
		assertEquals(1, results[2].size());
	}

}
