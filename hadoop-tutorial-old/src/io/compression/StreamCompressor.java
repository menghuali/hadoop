package io.compression;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * To run: echo "Text" | hadoop io.compression.StreamCompressor
 * org.apache.hadoop.io.compress.GzipCodec | gunzip
 *
 */
public class StreamCompressor {

	public static void main(String[] args) throws IOException {
		String codecClassname = args[0];
		CompressionOutputStream out = null;
		try {
			Class<?> codecClass = Class.forName(codecClassname);
			Configuration conf = new Configuration();
			CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			out = codec.createOutputStream(System.out);
			IOUtils.copyBytes(System.in, out, conf);
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null)
				out.finish();
		}
	}

}
