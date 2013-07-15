package nl.cwi.da.monetdb.loader.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

public class MonetDBStoreFunc implements StoreFuncInterface {

	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return LoadFunc.getAbsolutePath(location, curDir);
	}

	public static Logger log = Logger.getLogger(MonetDBStoreFunc.class);

	@SuppressWarnings("rawtypes")
	public OutputFormat getOutputFormat() throws IOException {
		return new MonetDBOutputFormat();
	}

	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

	private static Collection<Byte> supportedTypes = Arrays.asList(
			DataType.BOOLEAN, DataType.BYTE, DataType.INTEGER, DataType.LONG,
			DataType.FLOAT, DataType.DOUBLE, DataType.LONG, DataType.CHARARRAY);

	public void checkSchema(ResourceSchema s) throws IOException {
		for (ResourceFieldSchema rfs : s.getFields()) {
			if (!supportedTypes.contains(rfs.getType())) {
				throw new IOException("Unsupported Column type: "
						+ rfs.getName() + " (" + rfs.getType() + ") - Sorry!");
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private RecordWriter<WritableComparable, Tuple> w;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void prepareToWrite(RecordWriter writer) throws IOException {
		w = writer;
	}

	public void putNext(Tuple t) throws IOException {
		try {
			w.write(null, t);
		} catch (InterruptedException e) {
			log.warn(e);
		}
	}

	public void setStoreFuncUDFContextSignature(String signature) {
	}

	public void cleanupOnFailure(String location, Job job) throws IOException {
		// TODO delete files already generated!
	}

	public void cleanupOnSuccess(String location, Job job) throws IOException {
		// nothing to do here, we are finished when we are
	}

}
