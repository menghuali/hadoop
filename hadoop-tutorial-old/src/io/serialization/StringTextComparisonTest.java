package io.serialization;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class StringTextComparisonTest {

	@Test
	public void string() throws Throwable {
		String s = "\u0041\u00DF\u6711\uD801\uDC00";

		assertThat(s.length(), is(5));
		assertThat(s.getBytes("UTF-8").length, is(10));

		assertThat(s.indexOf("\u0041"), is(0));
		assertThat(s.indexOf("\u00DF"), is(1));
		assertThat(s.indexOf("\u6711"), is(2));
		assertThat(s.indexOf("\uD801\uDC00"), is(3));

		assertThat(s.charAt(0), is('\u0041'));
		assertThat(s.charAt(1), is('\u00DF'));
		assertThat(s.charAt(2), is('\u6711'));
		assertThat(s.charAt(3), is('\uD801'));
		assertThat(s.charAt(4), is('\uDC00'));

		assertThat(s.codePointAt(0), is(0x0041));
		assertThat(s.codePointAt(1), is(0x00DF));
		assertThat(s.codePointAt(2), is(0x6711));
		assertThat(s.codePointAt(3), is(0x10400));
	}

	@Test
	public void test() throws Throwable {
		Text t = new Text("\u0041\u00DF\u6711\uD801\uDC00");
		assertThat(t.getLength(), is(10));
		assertThat(new Text("\u0041").getLength(), is(1));
		assertThat(new Text("\u00DF").getLength(), is(2));
		assertThat(new Text("\u6711").getLength(), is(3));
		assertThat(new Text("\uD801\uDC00").getLength(), is(4));

		assertThat(t.find("\u0041"), is(0));
		assertThat(t.find("\u00DF"), is(1));
		assertThat(t.find("\u6711"), is(3));
		assertThat(t.find("\uD801\uDC00"), is(6));
		
		assertThat(t.charAt(0), is(0x0041));
		assertThat(t.charAt(1), is(0x00DF));
		assertThat(t.charAt(3), is(0x6711));
		assertThat(t.charAt(6), is(0x10400));
	}

}
