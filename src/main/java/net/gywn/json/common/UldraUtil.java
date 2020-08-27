package net.gywn.json.common;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.collections4.map.CaseInsensitiveMap;

import net.gywn.json.beans.BinlogColumn;

public class UldraUtil {
	private final static CaseInsensitiveMap<String, String> charMap = new CaseInsensitiveMap<String, String>();
	static {
		charMap.put("euckr", "MS949");
		charMap.put("utf8", "UTF-8");
		charMap.put("utf8mb4", "UTF-8");
	}

	public static long crc32(final String s) {
		Checksum checksum = new CRC32();
		try {
			byte[] bytes = s.getBytes();
			checksum.update(bytes, 0, bytes.length);
			return checksum.getValue();
		} catch (Exception e) {
		}
		return 0;
	}

	public static void sleep(final long sleepMili) {
		try {
			Thread.sleep(sleepMili);
		} catch (InterruptedException e2) {
		}
	}

	public static void writeFile(final String filename, final String info) {
		try {
			Files.write(Paths.get(filename), info.getBytes(StandardCharsets.UTF_8));
		} catch (Exception e) {
		}
	}

	public static String readFile(String path) {
		try {
			byte[] encoded = Files.readAllBytes(Paths.get(path));
			return new String(encoded, StandardCharsets.UTF_8);
		} catch (Exception e) {
		}
		return null;
	}

	private static String getMysqlDatetime(final Serializable serializable) {
		long time = (long) serializable;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return String.format("%s.%06d", format.format(new Date(time / 1000)), time % 1000000);
	}

	private static String getMysqlTimestamp(final Serializable serializable) {
		long time = (long) serializable;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return String.format("%s.%06d", format.format(new Date(time / 1000)), time % 1000000);
	}

	private static String getMysqlDate(final Serializable serializable) {
		long time = (long) serializable;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return String.format("%s", format.format(new Date(time / 1000)));
	}

	private static String getMysqlTime(final Serializable serializable) {
		long time = (long) serializable;
		SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return String.format("%s.%06d", format.format(new Date(time / 1000)), time % 1000000);
	}

	public static String toString(final Serializable serializable, final BinlogColumn column) {

		if (serializable == null) {
			return null;
		}

		switch (column.getType()) {
		case "datetime":
			return getMysqlDatetime(serializable);
		case "timestamp":
			return getMysqlTimestamp(serializable);
		case "date":
			return getMysqlDate(serializable);
		case "time":
			return getMysqlTime(serializable);
		}

		if (serializable instanceof String) {
			return (String) serializable;
		}

		if (serializable instanceof java.lang.Integer) {
			return column.isUnsigned() ? Integer.toUnsignedString((Integer) serializable) : serializable.toString();
		}

		if (serializable instanceof java.lang.Long) {
			return column.isUnsigned() ? Long.toUnsignedString((Long) serializable) : serializable.toString();
		}

		if (serializable instanceof byte[] && column.getCharset() != null) {
			return toCharsetString((byte[]) serializable, column.getCharset());
		}

		return serializable.toString();
	}

	public static String toCharsetString(final byte[] byteArray, final String mysqlCharset) {
		String javaCharset = charMap.get(mysqlCharset);
		if (javaCharset != null) {
			try {
				return new String(byteArray, javaCharset);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return new String(byteArray);
	}
}
