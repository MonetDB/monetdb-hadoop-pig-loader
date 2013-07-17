package nl.cwi.da.monetdb.loader.hadoop;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
import org.apache.pig.impl.util.ObjectSerializer;

public class MonetDBStoreFunc implements StoreFuncInterface {

	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return LoadFunc.getAbsolutePath(location, curDir);
	}

	public static Logger log = Logger.getLogger(MonetDBStoreFunc.class);

	private static final String SCHEMA_SER = ".schema.ser";
	private static final String SCHEMA_SQL = "schema.sql";
	private static final String LOADER_SQL = "load.sql";

	@SuppressWarnings("rawtypes")
	public OutputFormat getOutputFormat() throws IOException {
		return new MonetDBOutputFormat();
	}

	private MonetDBSQLSchema sqlSchema = new MonetDBSQLSchema();

	public void setStoreLocation(String location, Job job) throws IOException {
		Path p = new Path(location);
		FileOutputFormat.setOutputPath(job, p);
		FileSystem fs = p.getFileSystem(job.getConfiguration());

		sqlSchema.setTableName(p.getName());

		Path schemaPath = p.suffix("/" + SCHEMA_SQL);
		if (!fs.exists(schemaPath)) {
			FSDataOutputStream os = fs.create(schemaPath);
			os.write(sqlSchema.toSQL().getBytes());
			os.close();
		}
		log.info("Wrote SQL Schema to " + schemaPath);

		Path schemaSerPath = p.suffix("/" + SCHEMA_SER);
		if (!fs.exists(schemaSerPath)) {

			Map<String, Serializable> schemaMetaData = new HashMap<String, Serializable>();
			schemaMetaData.put("numCols", sqlSchema.getNumCols());
			schemaMetaData.put("tableName", p.getName());

			FSDataOutputStream os = fs.create(schemaSerPath);
			os.write(serialize(schemaMetaData).getBytes());
			os.close();
		}

	}

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

	private String signature;

	public void setStoreFuncUDFContextSignature(String signature) {
		this.signature = signature;
	}

	public void cleanupOnFailure(String location, Job job) throws IOException {
		Path p = new Path(location);
		FileSystem fs = p.getFileSystem(job.getConfiguration());
		fs.delete(p, true);
	}

	public void cleanupOnSuccess(String location, Job job) throws IOException {

		Path p = new Path(location);
		FileSystem fs = p.getFileSystem(job.getConfiguration());

		FileStatus[] partDirs = fs.listStatus(p, new PathFilter() {
			public boolean accept(Path arg0) {
				return arg0.getName().startsWith(
						MonetDBRecordWriter.FOLDER_PREFIX);
			}
		});

		Path schemaSerPath = p.suffix("/" + SCHEMA_SER);
		String schemaSerStr = IOUtils.toString(fs.open(schemaSerPath), "UTF-8");
		@SuppressWarnings("unchecked")
		Map<String, Object> schemaMetaData = (Map<String, Object>) deserialize(schemaSerStr);
		int numCols = (Integer) schemaMetaData.get("numCols");
		String tableName = (String) schemaMetaData.get("tableName");

		String loader = "";

		for (FileStatus fis : partDirs) {
			loader += "COPY BINARY INTO \"" + tableName + "\" FROM (\n";
			Path cp = fis.getPath();
			for (int colId = 0; colId < numCols; colId++) {
				Path cpp = cp.suffix("/" + MonetDBRecordWriter.FILE_PREFIX
						+ colId + MonetDBRecordWriter.FILE_SUFFIX);
				if (!fs.exists(cpp)) {
					throw new IOException("Need path " + cpp
							+ ", but is non-existent.");
				}
				String colpartName = cp.getName() + "/" + cpp.getName();
				loader += "'$PATH/" + colpartName + "'";
				if (colId < numCols - 1) {
					loader += ",\n";
				}

			}
			loader += "\n);\n";
		}

		Path loaderPath = p.suffix("/" + LOADER_SQL);
		if (!fs.exists(loaderPath)) {
			FSDataOutputStream os = fs.create(loaderPath);
			os.write(loader.getBytes());
			os.close();
		}
		log.info("Wrote SQL Loader to " + loaderPath);

		fs.delete(p.suffix("/" + SCHEMA_SER), false);
	}

	public static String serialize(Object o) throws IOException {
		return ObjectSerializer.serialize((Serializable) o);
	}

	public static Object deserialize(String schemaStr) throws IOException {
		return ObjectSerializer.deserialize(schemaStr);
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

}
