package myutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MROutputReader {

	public static Map<String, String> readMap(Reader in, String separator) throws Exception {
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

	public static Map<String, String> readMap(File file, String separator) throws Exception {
		return readMap(new FileReader(file), separator);
	}

	public static Map<String, String> readMap(File file) throws Exception {
		return readMap(file, "\t");
	}

	public static List<String> readLines(Reader in) throws Exception {
		ArrayList<String> result = new ArrayList<>();
		BufferedReader reader = new BufferedReader(in);
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				result.add(line);
			}
		} finally {
			reader.close();
		}
		return result;
	}

	public static List<String> readLines(File file) throws Exception {
		return readLines(new FileReader(file));
	}

}
