package hdfs.file.write;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {

	public static void main(String[] args) {
		String src = args[0];
		String dst = args[1];

		InputStream in = null;
		OutputStream out = null;
		try {
			in = new BufferedInputStream(new FileInputStream(src));
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(dst), conf);
			out = fs.create(new Path(dst), new Progressable() {
				@Override
				public void progress() {
					System.out.print(".");

				}
			});
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
