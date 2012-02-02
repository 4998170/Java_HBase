import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

			HTableDescriptor[] tableList = admin.listTables();

			FileOutputStream out = new FileOutputStream(
					"HBase_schema_description.txt");
			PrintStream ps = new PrintStream(out);

			for (int i = 0; i < tableList.length; i++) {

				String tableName = tableList[i].getNameAsString();

				Set<String> tableNames = new HashSet<String>();
				tableNames.add(tableName);

				System.out.println("Table: " + tableName);
				ps.println("\n");
				ps.println("Table: " + tableName);

				HTable table = new HTable(config, tableName);
				Scan s = new Scan();

				ResultScanner scanner = table.getScanner(s);
				try {
					HashSet<String> colfamSet = new HashSet<String>();
					HashMap<String, Integer> columnMap = new HashMap<String, Integer>();
					HashSet<String> rowkeySet = new HashSet<String>();

					for (Result rr : scanner) {
						rowkeySet.add(Bytes.toString(rr.getRow()));

						List<KeyValue> list = rr.list();
						for (Iterator<KeyValue> ik = list.iterator(); ik
								.hasNext();) {
							KeyValue kv = ik.next();
							String colfam = Bytes.toString(kv.getFamily());
							String qualifier = Bytes
									.toString(kv.getQualifier());

							colfamSet.add(colfam);
							if (columnMap.containsKey(colfam + ":" + qualifier)) {
								Integer colValue = columnMap.get(colfam + ":"
										+ qualifier);
								columnMap.put(colfam + ":" + qualifier,
										colValue + 1);
							} else {
								columnMap.put(colfam + ":" + qualifier, 1);
							}

						}
					}

					System.out.println("Number of column families: "
							+ colfamSet.size());
					ps
							.println("Number of column families: "
									+ colfamSet.size());

					System.out.println("Column families:");
					ps.println("Column families:");

					Iterator<String> it = colfamSet.iterator();

					while (it.hasNext())
						// next line gives an error???
						// String colfam = it.next();
						// System.out.println(it.next());
						ps.println(it.next());
					// System.out.println(colfam);
					// ps.println(colfam);

					System.out
							.println("Columns and number of rows that contain this column:");
					ps
							.println("Columns and number of rows that contain this column:");

					for (Map.Entry<String, Integer> entry : columnMap
							.entrySet()) {
						System.out.println(entry.getKey() + ", Count = "
								+ entry.getValue());
						ps.println(entry.getKey() + ", Count = "
								+ entry.getValue());
					}

					System.out.println("Number of rows scanned = "
							+ rowkeySet.size());
					ps.println("Number of rows scanned = " + rowkeySet.size());

				} finally {
					scanner.close();

				}

			}

			out.close();

		} catch (MasterNotRunningException e) {
			System.out
					.println("Could not setup HBaseAdmin as no master is running, did you start HBase?...");
		} catch (ZooKeeperConnectionException e) {
			System.out.println("Could not connect to ZooKeeper");
		} catch (IOException e) {
			System.out.println("Caught IOException: " + e.getMessage());
		}
	}
}
