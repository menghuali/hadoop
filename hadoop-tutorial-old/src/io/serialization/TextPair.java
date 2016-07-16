package io.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {

	private Text first;
	private Text second;

	public TextPair() {
		set(new Text(), new Text());
	}

	public TextPair(String first, String second) {
		set(new Text(first), new Text(second));
	}

	public TextPair(Text first, Text second) {
		set(first, second);
	}

	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public int compareTo(TextPair pair) {
		int cmp = first.compareTo(pair.first);
		if (cmp == 0) {
			return second.compareTo(pair.second);
		} else {
			return cmp;
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextPair) {
			TextPair pair = (TextPair) obj;
			return first.equals(pair.first) && second.equals(pair.second);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public String toString() {
		return first.toString() + '\t' + second.toString();
	}

}
