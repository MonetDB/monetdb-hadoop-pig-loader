package nl.cwi.da.monetdb.loader.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
		Path p = new Path(location);
		FileOutputFormat.setOutputPath(job, p);
		sqlSchema.setTableName(p.getName());

		Path schemaPath = p.suffix("/_schema.sql");
		Path loaderPath = p.suffix("/_loader.sql");

		FileSystem fs = p.getFileSystem(job.getConfiguration());

		if (!fs.exists(schemaPath)) {
			FSDataOutputStream os = fs.create(schemaPath);
			os.write(sqlSchema.toSQL().getBytes());
			os.close();
			log.info("Wrote SQL Schema to " + schemaPath);
		}
		
		if (!fs.exists(loaderPath)) {
			FSDataOutputStream os = fs.create(loaderPath);
			os.write(sqlSchema.getLoaderSQL().getBytes());
			os.close();
			log.info("Wrote MonetDB loading command to " + loaderPath);
		}
	}

	private static Map<Byte, String> pigSqlTypeMap = new HashMap<Byte, String>();
	static {
		pigSqlTypeMap.put(DataType.BOOLEAN, "BOOLEAN");
		pigSqlTypeMap.put(DataType.BYTE, "TINYINT");
		pigSqlTypeMap.put(DataType.INTEGER, "INT");
		pigSqlTypeMap.put(DataType.LONG, "BIGINT");
		pigSqlTypeMap.put(DataType.FLOAT, "REAL");
		pigSqlTypeMap.put(DataType.DOUBLE, "DOUBLE");
		pigSqlTypeMap.put(DataType.CHARARRAY, "CLOB");
	}

	private MonetDBSQLSchema sqlSchema = new MonetDBSQLSchema();

	public void checkSchema(ResourceSchema s) throws IOException {
		boolean addColumns = sqlSchema.getNumCols() == 0;

		for (ResourceFieldSchema rfs : s.getFields()) {
			if (!pigSqlTypeMap.containsKey(rfs.getType())) {
				throw new IOException("Unsupported Column type: "
						+ rfs.getName() + " (" + rfs.getType() + ") - Sorry!");
			}
			if (addColumns) {
				sqlSchema.addColumn(rfs.getName(),
						pigSqlTypeMap.get(rfs.getType()));
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
