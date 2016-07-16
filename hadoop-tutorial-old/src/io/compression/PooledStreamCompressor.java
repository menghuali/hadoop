package io.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * To run: echo "Text" | hadoop io.compression.PooledStreamCompressor
 * org.apache.hadoop.io.compress.GzipCodec | gunzip
 *
 */
public class PooledStreamCompressor {

	public static void main(String[] args) throws Exception {
		Compressor compressor = null;
		CompressionOutputStream out = null;
		try {
			String codecClassname = args[0];
			Class<?> codecClass = Class.forName(codecClassname);
			Configuration conf = new Configuration();
			CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			compressor = CodecPool.getCompressor(codec);
			out = codec.createOutputStream(System.out, compressor);
			IOUtils.copyBytes(System.in, out, conf);
		} finally {
			out.finish();
			CodecPool.returnCompressor(compressor);
		}
	}

}
