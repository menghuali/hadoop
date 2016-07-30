package myutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

public class MROutputReader {

	public static Map<String, String> read(Reader in, String separator) throws Exception {
		HashMap<String, String> result = new HashMap<>();
		BufferedReader reader = new BufferedReader(in);
		String line = null;
		String[] split = null;
		try {
			while ((line = reader.readLine()) != null) {
				split = line.split(separator);
				result.put(split[0], split[1]);
			}
		} finally {
			reader.close();
		}
		return result;
	}

	public static Map<String, String> read(File file, String separator) throws Exception {
		return read(new FileReader(file), separator);
	}

	public static Map<String, String> read(File file) throws Exception {
		return read(file, "\t");
	}

}
