package nl.cwi.da.monetdb.loader.hadoop.test;

import java.io.IOException;

import nl.cwi.da.monetdb.loader.hadoop.MonetDBSQLSchema;

import org.apache.pig.impl.util.ObjectSerializer;
import org.junit.Test;

public class SerializerTest {

	@Test
	public void serializerTest() throws IOException {
		MonetDBSQLSchema schema = new MonetDBSQLSchema("supplier");
		schema.addColumn("s_supplier", "INT");
		schema.addColumn("s_name", "VARCHAR(25)");
		schema.addColumn("s_address", "VARCHAR(40)");
		System.out.println(schema);

		String ser = ObjectSerializer.serialize(schema);
		System.out.println(ser);
		MonetDBSQLSchema schema2 = (MonetDBSQLSchema) ObjectSerializer
				.deserialize(ser);
		System.out.println(schema2);

		String schemaTest = "eNqVT7tOAkEUvcAKAS2M1nbWgz0VCxYkI8SANbnsTmB1XuzclcXCxM+g8A/8D3v/w39wZglRSqe4yZxzzz3nfHzDicvhRkuWbDKWIlNGC0oXTBpMRc5WmBpj2V1Ah/H0nk+TlVAI+1erQ51DKzGyUNoRnPNHfMZuQZns8sxRj0ObcCHFGJUguNjTEvWyO6U808teab39ZYBZULF+nuM2SMu3r6vdJ743oDaCyGUvorTesbWJ/Ox4Uf+/ma9/gUEV+KhEJ5TQKgRdw2tVy63lbGv339IStN3cFdY+iS1BYzSeucA0CZpurqt+0YBP4gPqtzFNc+Eq4OwAn4Zlyoz2ZwLUPhAtN7crn/BoOxxJElqg9DbDyUPMb/9QiVFKaKoUpb/gzQpJ7geq/I9H";
		MonetDBSQLSchema schema3 = (MonetDBSQLSchema) ObjectSerializer
				.deserialize(schemaTest);
		System.out.println(schema3);

	}

}
