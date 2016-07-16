package io.serialization;

import static org.junit.Assert.*;
import static org.hamcrest.core.Is.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestWritable {

	@Test
	public void test() throws Exception {
		byte[] bytes = serialize(new IntWritable(163));
		assertThat(bytes.length, is(4));
		assertThat(StringUtils.byteToHexString(bytes), is("000000a3"));
		IntWritable writable = new IntWritable();
		deserialize(writable, bytes);
		assertThat(writable.get(), is(163));
	}

	static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(bout);
		writable.write(dout);
		return bout.toByteArray();
	}

	static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
		ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(bin);
		writable.readFields(din);
		return bytes;
	}

}
