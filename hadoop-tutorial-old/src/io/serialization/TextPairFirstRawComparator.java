package io.serialization;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextPairFirstRawComparator extends WritableComparator {

	private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

	static {
		WritableComparator.define(TextPair.class, new TextPairFirstRawComparator());
	}

	public TextPairFirstRawComparator() {
		super(TextPair.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// How Text is serialized...
		// WritableUtils.writeVInt(out, length);
		// out.write(bytes, 0, length);
		try {
			// the size the VInt that stores the length of the Text +
			// the length of the Text itself.
			int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
			int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);

			return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
		} catch (Throwable e) {
			throw new IllegalArgumentException(e);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof TextPair && b instanceof TextPair)
			return ((TextPair) a).getFirst().compareTo(((TextPair) b).getFirst());
		else
			return super.compare(a, b);
	}

	public static void main(String[] args) throws IOException {
		TextPair tp1 = new TextPair("\u0061\u0062\u0063", "\u0041\u0042\u0043");
		ByteArrayOutputStream bout1 = new ByteArrayOutputStream();
		DataOutputStream dout1 = new DataOutputStream(bout1);
		tp1.write(dout1);
		byte[] b1 = bout1.toByteArray();

		TextPair tp2 = new TextPair("\u0061\u0062\u0063", "\u0061\u0062\u0063");
		ByteArrayOutputStream bout2 = new ByteArrayOutputStream();
		DataOutputStream dout2 = new DataOutputStream(bout2);
		tp2.write(dout2);
		byte[] b2 = bout2.toByteArray();

		TextPairFirstRawComparator comp = new TextPairFirstRawComparator();
		System.out.println("expected: " + "\u0061\u0062\u0063".compareTo("\u0061\u0062\u0063"));
		System.out.println("actual-binary: " + comp.compare(b1, 0, b1.length, b2, 0, b2.length));
		System.out.println("actual-object: " + comp.compare(tp1, tp2));

	}

}
