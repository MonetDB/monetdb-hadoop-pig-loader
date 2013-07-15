package nl.cwi.da.monetdb.loader.hadoop.test;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

public class MonetDBAttachTest {

	private static long NUM_VALUES = 100000000;
	private static String FILE_PREFIX = "/export/scratch1/hannes/tmp/";

	@Test
	public void intTest() throws IOException {
		Random r = new Random();

		BufferedOutputStream bosInt8 = new BufferedOutputStream(
				new FileOutputStream(FILE_PREFIX + "int8.bin"));
		ByteBuffer bbInt8 = ByteBuffer.allocate(1);

		BufferedOutputStream bosInt16 = new BufferedOutputStream(
				new FileOutputStream(FILE_PREFIX + "int16.bin"));
		ByteBuffer bbInt16 = ByteBuffer.allocate(2);

		BufferedOutputStream bosInt32 = new BufferedOutputStream(
				new FileOutputStream(FILE_PREFIX + "int32.bin"));
		ByteBuffer bbInt32 = ByteBuffer.allocate(4);

		BufferedOutputStream bosInt64 = new BufferedOutputStream(
				new FileOutputStream(FILE_PREFIX + "int64.bin"));
		ByteBuffer bbInt64 = ByteBuffer.allocate(8);

		BufferedOutputStream bosFloat = new BufferedOutputStream(
				new FileOutputStream(FILE_PREFIX + "float.bin"));
		ByteBuffer bbFloat = ByteBuffer.allocate(4);

		BufferedOutputStream bosDouble = new BufferedOutputStream(
				new FileOutputStream(FILE_PREFIX + "double.bin"));
		ByteBuffer bbDouble = ByteBuffer.allocate(8);

		/*BufferedOutputStream bosString = new BufferedOutputStream(
				new FileOutputStream(FILE_PREFIX + "string.bin"));*/

		for (long i = 0; i < NUM_VALUES; i++) {
			bosInt8.write(bbInt8.put((byte) (r.nextFloat() * 100)).array());
			bbInt8.clear();

			bosInt16.write(bbInt16.putShort((short) (r.nextFloat() * 10000))
					.array());
			bbInt16.clear();

			bosInt32.write(bbInt32.putInt(r.nextInt()).array());
			bbInt32.clear();

			bosInt64.write(bbInt64.putLong(r.nextLong()).array());
			bbInt64.clear();

			bosFloat.write(bbFloat.putFloat(r.nextFloat()).array());
			bbFloat.clear();

			bosDouble.write(bbDouble.putDouble(r.nextDouble()).array());
			bbDouble.clear();

		/*	bosString.write(RandomStringUtils.randomAlphanumeric(
					r.nextInt(1000)).getBytes());
			bosString.write("\n".getBytes()); */

		}
		bosInt8.close();
		bosInt16.close();
		bosInt32.close();
		bosInt64.close();
		bosFloat.close();
		bosDouble.close();
		//bosString.close();

		System.out.println("DROP TABLE attachtest;");
		System.out
				.println("CREATE TABLE attachtest (int8 TINYINT,int16 SMALLINT, int32 INTEGER, int64 BIGINT, flt REAL, dbl DOUBLE);");
		System.out.println("COPY BINARY INTO attachtest FROM('" + FILE_PREFIX
				+ "int8.bin','" + FILE_PREFIX + "int16.bin','" + FILE_PREFIX
				+ "int32.bin','" + FILE_PREFIX + "int64.bin','" + FILE_PREFIX
				+ "float.bin','" + FILE_PREFIX + "double.bin');");

	}
	// http://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
	// http://www.monetdb.org/Documentation/Manuals/SQLreference/BuiltinTypes
}
