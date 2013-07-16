package nl.cwi.da.monetdb.loader.hadoop.test;

import nl.cwi.da.monetdb.loader.hadoop.MonetDBSQLSchema;

import org.junit.Test;

public class SchemaTest {

	@Test
	public void schemaTest() {
		MonetDBSQLSchema schema = new MonetDBSQLSchema("supplier");
		schema.addColumn("s_supplier", "INT");
		schema.addColumn("s_name", "VARCHAR(25)");
		schema.addColumn("s_address", "VARCHAR(40)");
		schema.addColumn("s_nationkey", "INT");
		schema.addColumn("s_phone", "VARCHAR(15)");
		schema.addColumn("s_acctbal", "DECIMAL(15,2)");
		schema.addColumn("s_comment", "VARCHAR(101)");
		
		System.out.println(schema.toSQL());
		System.out.println(schema.getLoaderSQL());

	}
}
