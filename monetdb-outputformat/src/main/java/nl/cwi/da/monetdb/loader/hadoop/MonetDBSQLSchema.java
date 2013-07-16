package nl.cwi.da.monetdb.loader.hadoop;

import java.util.ArrayList;
import java.util.List;

public class MonetDBSQLSchema {
	public static class MonetDBSQLColumn {
		public String colunmName;
		public String sqlType;

		public MonetDBSQLColumn(String columnName, String sqlType) {
			this.colunmName = columnName;
			this.sqlType = sqlType.toUpperCase();
		}

		public String toSQL() {
			return "\"" + colunmName + "\" " + sqlType;
		}
	}

	private List<MonetDBSQLColumn> columns = new ArrayList<MonetDBSQLColumn>();
	private String tableName;

	public MonetDBSQLSchema(String tableName) {
		this.tableName = tableName;
	}

	public MonetDBSQLSchema() {
	}

	public MonetDBSQLSchema setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public String toSQL() {
		String ret = "CREATE TABLE \"" + tableName + "\" (\n";

		for (int i = 0; i < columns.size(); i++) {
			MonetDBSQLColumn col = columns.get(i);
			ret += col.toSQL();
			if (i < columns.size() - 1) {
				ret += ",\n";
			}
		}

		ret += "\n);\n";
		return ret;
	}

	public String getLoaderSQL() {
		String ret = "COPY BINARY INTO \"" + tableName + "\" FROM (\n";

		for (int i = 0; i < columns.size(); i++) {
			ret += "'$PATH/" + MonetDBRecordWriter.FILE_PREFIX + i
					+ MonetDBRecordWriter.FILE_SUFFIX + "'";
			if (i < columns.size() - 1) {
				ret += ",\n";
			}
		}

		ret += "\n);\n";
		return ret;
	}

	public MonetDBSQLSchema addColumn(String columnName, String sqlType) {
		columns.add(new MonetDBSQLColumn(columnName, sqlType));
		return this;
	}

	public int getNumCols() {
		return columns.size();
	}
}
