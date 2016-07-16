package hdfs.file.read;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat {

	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		InputStream in = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}

}
