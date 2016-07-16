package hdfs.file.read;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemDoubleCat {

	public static void main(String[] args) {
		String uri = args[0];
		Configuration cfg = new Configuration();
		FSDataInputStream in = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), cfg);
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0);
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}

}
