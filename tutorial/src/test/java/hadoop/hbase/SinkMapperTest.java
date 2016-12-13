package hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.text.ParseException;

import org.junit.Test;

public class SinkMapperTest extends SinkMapper {

//	@Test
//	public void testMapLongWritableTextContext() throws IOException {
//		MapDriver<LongWritable, Text, ImmutableBytesWritable, Put> driver = new MapDriver<>();
//		driver.withMapper(new SinkMapper());
//		driver.withInput(new LongWritable(0), new Text("416-537-4494..123 Pizza…………………1172 Bloor St W…"));
//		driver.withInput(new LongWritable(1), new Text("416-516-6625..2-4-1 Pizza……………….1383 Davenport Rd…"));
//
//		Put put0 = new Put(Bytes.toBytes("416-537-4494"));
//		put0.addColumn(SinkMapper.FAMILY, SinkMapper.BIZ_NAME, Bytes.toBytes("123 Pizza"));
//		put0.addColumn(SinkMapper.FAMILY, SinkMapper.ADDRESS, Bytes.toBytes("1172 Bloor St W…"));
//		driver.withOutput(new ImmutableBytesWritable(put0.getRow()), put0);
//		
//		Put put1 = new Put(Bytes.toBytes("416-516-6625"));
//		put1.addColumn(SinkMapper.FAMILY, SinkMapper.BIZ_NAME, Bytes.toBytes("2-4-1 Pizza"));
//		put1.addColumn(SinkMapper.FAMILY, SinkMapper.ADDRESS, Bytes.toBytes("1383 Davenport Rd…"));
//		driver.withOutput(new ImmutableBytesWritable(put1.getRow()), put1);
//		
//		driver.runTest();
//	}

	@Test
	public void testParseLine() throws ParseException {
		String[] result = SinkMapper.parseLine("416-537-4494..123 Pizza…………………1172 Bloor St W…");
		assertNotNull("result is null", result);
		assertEquals("the size of result isn't 3", 3, result.length);
		assertEquals("Phone isn't expected", "416-537-4494", result[0]);
		assertEquals("Busines name isn't expected", "123 Pizza", result[1]);
		assertEquals("Address isn't expected", "1172 Bloor St W…", result[2]);
	}

}
