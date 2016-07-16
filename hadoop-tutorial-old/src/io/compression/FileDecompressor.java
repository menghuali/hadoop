package io.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class FileDecompressor {

	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(path);
		if (codec == null) {
			System.err.println("No codec for " + uri);
			System.exit(1);
		}

		InputStream in = null;
		OutputStream out = null;
		try {
			String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
			in = codec.createInputStream(fs.open(path));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		} finally {
			in.close();
			out.close();
		}
	}

}
