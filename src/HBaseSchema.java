package bigdata.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSchema {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration config = HBaseConfiguration.create();

		try {
			HBaseAdmin admin = new HBaseAdmin(config);

			HTableDescriptor[] descList = admin.listTables();

			for (int i = 0; i < descList.length; i++) {
				HTableDescriptor desc = descList[i];
				String tableName = desc.getNameAsString();
				System.out.println("Table: " + tableName);

				// todo: change the code below so 2 new collections are defined
				// imediately a table scan is done (so columndescriptors are
				// bypassed
				// in one collection the columnfamilynames are stored, in the
				// other the columnfamilyname and columnnames.
				Collection<HColumnDescriptor> fam = desc.getFamilies();

				for (Iterator<HColumnDescriptor> it = fam.iterator(); it
						.hasNext();) {
					HColumnDescriptor famcoldesc = it.next();
					String famcol = famcoldesc.getNameAsString();
					System.out.println("Column family: " + famcol);

					HTable table = new HTable(config, tableName);
					Scan s = new Scan();
					// Get all columns from the specified family.
					s.addFamily(Bytes.toBytes(famcol));
					// s.addFamily(Bytes.toBytes("blogposts"));

					ResultScanner scanner = table.getScanner(s);
					try {
						// Scanners return Result instances.
						// Now, for the actual iteration. One way is to use a
						// while loop like so:
						/*
						 * for (Result rr = scanner.next(); rr != null; rr =
						 * scanner .next()) { // print out the row we found and
						 * the columns we // were looking for
						 * System.out.println("Found row: " + rr); }
						 */
						// The other approach is to use a foreach loop. Scanners
						// are iterable!
						for (Result rr : scanner) {
							// System.out.println("Found row: " + rr);
							List<KeyValue> list = rr.list();
							for (Iterator<KeyValue> ik = list.iterator(); ik
									.hasNext();) {
								KeyValue kv = ik.next();
								String colfam = Bytes.toString(kv.getFamily());
								String qualifier = Bytes.toString(kv
										.getQualifier());
								System.out.println("Columnfamily:" + colfam
										+ " Columnname:" + qualifier);
							}
						}
					} finally {
						// Make sure you close your scanners when you are done!
						// Thats why we have it inside a try/finally clause
						scanner.close();
					}
				}

				/*
				 * HTableDescriptor htd1 = admin.getTableDescriptor(Bytes
				 * .toBytes(desc.getNameAsString())); System.out.println(htd1);
				 */

			}

			/*
			 * if (!admin.tableExists("testTable")) { admin.createTable(new
			 * HTableDescriptor("testTable"));
			 * 
			 * // disable so we can make changes to it
			 * admin.disableTable("testTable");
			 * 
			 * // lets add 2 columns admin .addColumn("testTable", new
			 * HColumnDescriptor( "firstName")); admin.addColumn("testTable",
			 * new HColumnDescriptor("lastName"));
			 * 
			 * // enable the table for use admin.enableTable("testTable");
			 * 
			 * }
			 */

			// get the table so we can use it in the next set of examples
			// HTable table = new HTable(config, "testTable");
		} catch (MasterNotRunningException e) {
			// throw new Exception(
			System.out
					.println("Could not setup HBaseAdmin as no master is running, did you start HBase?...");
		} catch (ZooKeeperConnectionException e) {
			// throw new Exception
			System.out.println("Could not connect to ZooKeeper");
		} catch (IOException e) {
			// throw new Exception
			System.out.println("Caught IOException: " + e.getMessage());
		}
	}
}
