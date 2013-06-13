package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

public class DeleteTweetsTable {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{

		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
		HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);
		TweetsTable table = new TweetsTable(pool);
		table.deleteTweetsTable();
	}
}
