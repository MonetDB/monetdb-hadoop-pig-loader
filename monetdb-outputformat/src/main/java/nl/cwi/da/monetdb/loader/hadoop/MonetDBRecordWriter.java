package nl.cwi.da.monetdb.loader.hadoop;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.data.Tuple;

@SuppressWarnings("rawtypes")
public class MonetDBRecordWriter extends
		RecordWriter<WritableComparable, Tuple> {

	public static Logger log = Logger.getLogger(MonetDBRecordWriter.class);

	private TaskAttemptContext context;

	private Map<Integer, ValueConverter> converters = new HashMap<Integer, ValueConverter>();

	public MonetDBRecordWriter(TaskAttemptContext context) throws IOException {
		this.context = context;

	}

	private Map<Integer, OutputStream> writers = new HashMap<Integer, OutputStream>();
	boolean writersInitialized = false;

	private interface ValueConverter {
		byte[] convert(Object value);

	}

	private static class BooleanValueConverter implements ValueConverter {
		private ByteBuffer bb = ByteBuffer.allocate(1);

		public byte[] convert(Object value) {
			bb.clear();
			Boolean val = (Boolean) value;

			if (val == null) {
				bb.put((byte) Byte.MIN_VALUE);
			}
			if (val == true) {
				bb.put((byte) 1);
			}
			if (val == false) {
				bb.put((byte) 0);
			}
			bb.put((Byte) value);
			return bb.array();
		}
	}

	private static class ByteValueConverter implements ValueConverter {
		private ByteBuffer bb = ByteBuffer.allocate(1);

		public byte[] convert(Object value) {
			bb.clear();
			if (value == null) {
				value = Byte.MIN_VALUE;
			}
			bb.put((Byte) value);
			return bb.array();
		}
	}

	private static class ShortValueConverter implements ValueConverter {
		private ByteBuffer bb = ByteBuffer.allocate(2);

		public byte[] convert(Object value) {
			bb.clear();
			if (value == null) {
				value = Byte.MIN_VALUE;
			}
			bb.putShort((Short) value);
			return bb.array();
		}
	}

	private static class IntegerValueConverter implements ValueConverter {
		private ByteBuffer bb = ByteBuffer.allocate(4);

		public byte[] convert(Object value) {
			bb.clear();
			if (value == null) {
				value = Integer.MIN_VALUE;
			}
			bb.putInt((Integer) value);
			return bb.array();
		}
	}

	private static class LongValueConverter implements ValueConverter {
		private ByteBuffer bb = ByteBuffer.allocate(8);

		public byte[] convert(Object value) {
			bb.clear();
			if (value == null) {
				value = Long.MIN_VALUE;
			}
			bb.putLong((Long) value);
			return bb.array();
		}
	}

	private static class FloatValueConverter implements ValueConverter {
		private ByteBuffer bb = ByteBuffer.allocate(4);

		public byte[] convert(Object value) {
			bb.clear();
			if (value == null) {
				value = Float.MIN_VALUE;
			}
			bb.putFloat((Float) value);
			return bb.array();
		}
	}

	private static class DoubleValueConverter implements ValueConverter {
		private ByteBuffer bb = ByteBuffer.allocate(8);

		public byte[] convert(Object value) {
			bb.clear();
			if (value == null) {
				value = Double.MIN_VALUE;
			}
			bb.putDouble((Double) value);
			return bb.array();
		}
	}

	private static class StringValueConverter implements ValueConverter {
		public byte[] convert(Object value) {
			return (((String) value) + "\n").getBytes();
		}
	}

	public void write(WritableComparable key, Tuple t) throws IOException {

		if (!writersInitialized) {
			for (int i = 0; i < t.size(); i++) {

				Path outPath = FileOutputFormat.getOutputPath(context);
				FileSystem fs = outPath.getFileSystem(context
						.getConfiguration());

				OutputStream os = fs.create(outPath.suffix("/monetdb-load-" + i
						+ ".bulkload"));
				writers.put(i, os);

				Class valueClass = t.get(i).getClass();
				if (valueClass.equals(Boolean.class)) {
					converters.put(i, new BooleanValueConverter());
				}

				if (valueClass.equals(Byte.class)) {
					converters.put(i, new ByteValueConverter());
				}

				if (valueClass.equals(Short.class)) {
					converters.put(i, new ShortValueConverter());
				}

				if (valueClass.equals(Integer.class)) {
					converters.put(i, new IntegerValueConverter());
				}

				if (valueClass.equals(Long.class)) {
					converters.put(i, new LongValueConverter());
				}

				if (valueClass.equals(Float.class)) {
					converters.put(i, new FloatValueConverter());
				}

				if (valueClass.equals(Double.class)) {
					converters.put(i, new DoubleValueConverter());
				}

				if (valueClass.equals(String.class)) {
					converters.put(i, new StringValueConverter());
				}

				if (!converters.containsKey(i)) {
					throw new IOException(
							"Unable to fill converter table. Supported values are Java primitive types and Strings!");
				}

			}
			writersInitialized = true;
		}

		// TODO: check that the maps have a mapping there?
		for (int i = 0; i < t.size(); i++) {
			writers.get(i).write(converters.get(i).convert(t.get(i)));
		}
		context.progress();
	}

	@Override
	public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
		for (Entry<Integer, OutputStream> e : writers.entrySet()) {
			e.getValue().close();
		}

	}
}