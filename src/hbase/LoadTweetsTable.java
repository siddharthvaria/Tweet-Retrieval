package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;


public class LoadTweetsTable {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("Not enough arguments");
			System.out.println("Usage: TweetsLoader ##Tweets File Directory## ##Number Of Files##");
			System.exit(-1);
		}

		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
		HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);
		TweetsTable table = new TweetsTable(pool);
		table.InitTweetsTable();
		long numberOfTweets = table.readTweetsFromDirectory(args[0],Integer.parseInt(args[1]));
		System.out.println(numberOfTweets + " tweets loaded into tweets table!");
	}

}
