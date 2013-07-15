package nl.cwi.da.monetdb.loader.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.pig.data.Tuple;

@SuppressWarnings("rawtypes")
public class MonetDBOutputFormat extends
		OutputFormat<WritableComparable, Tuple> {

	public static Logger log = Logger.getLogger(MonetDBOutputFormat.class);

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException,
			InterruptedException {
		// TODO do we need this?
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		// TODO do we need this?
		return null;
	}

	@Override
	public RecordWriter<WritableComparable, Tuple> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {

		log.info(job);
		return new MonetDBRecordWriter(job);

	}
}