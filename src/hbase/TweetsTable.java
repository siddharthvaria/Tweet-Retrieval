package hbase;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
//import java.util.Scanner;
//import java.util.LinkedList;
//import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.json.DataObjectFactory;
public class TweetsTable {

	private HTablePool pool;
	//private static final Logger log = Logger.getLogger(TweetsTable.class);
	final String delimiter = "### ### ### ### ###";

	public final byte[] TWEETS = Bytes.toBytes("tweets");
	public final String[] FAMILIES = {"info","entity","geo","place","user"};
	//info family
	public static final byte[] TWEETS_INFO = Bytes.toBytes("info"); 	//tweets.info column family
	public static final byte[] TWEETS_INFO_TEXT = Bytes.toBytes("text"); 	//tweets.info.text (string)
	public static final byte[] TWEETS_INFO_CREATED = Bytes.toBytes("created"); 	//tweets.info.created (Date.toString)
	public static final byte[] TWEETS_INFO_SENSITIVE = Bytes.toBytes("sensitive");	//tweets.info.sensitive column (boolean)
	public static final byte[] TWEETS_INFO_RTC = Bytes.toBytes("rtc");				//tweets.info.retweetcount (long or -1)
	//entity family
	public static final byte[] TWEETS_ENTITY = Bytes.toBytes("entity");		//tweets.entity column family
	public static final byte[] TWEETS_ENTITY_URLS = Bytes.toBytes("urls");	//tweets.entity.urls (comma seperated list of urls)
	public static final byte[] TWEETS_ENTITY_HTAGS = Bytes.toBytes("htags");	//tweets.entity.htags (comma seperated list of hashtags)

	//geo family
	public static final byte[] TWEETS_GEO = Bytes.toBytes("geo");		//tweets.geo column family
	public static final byte[] TWEETS_GEO_LAT = Bytes.toBytes("lat"); //tweets.geo.lat (latitude(double))
	public static final byte[] TWEETS_GEO_LON = Bytes.toBytes("lon");	//tweets.geo.lon (longitude(double))
	//place family
	public static final byte[] TWEETS_PLACE = Bytes.toBytes("place");	//tweets.place column family
	public static final byte[] TWEETS_PLACE_COUNTRY = Bytes.toBytes("country"); //tweets.place.country (string)
	public static final byte[] TWEETS_PLACE_NAME = Bytes.toBytes("name");	//tweets.place.name (string)
	//user family
	public static final byte[] TWEETS_USER = Bytes.toBytes("user");	//tweets.user column family
	public static final byte[] TWEETS_USER_ID = Bytes.toBytes("id");	//tweets.user.id (long)
	public static final byte[] TWEETS_USER_LANG = Bytes.toBytes("lang");	//tweets.user.lang (string)
	public static final byte[] TWEETS_USER_NAME = Bytes.toBytes("name");	//tweets.user.name (string)
	public static final byte[] TWEETS_USER_SNAME = Bytes.toBytes("sname");	//tweets.user.screenname (string)
	public static final byte[] TWEETS_USER_URL = Bytes.toBytes("url");	//tweets.user.url (string)
	public static final byte[] TWEETS_USER_LOC = Bytes.toBytes("loc");	//tweets.user.location (string)
	public static final byte[] TWEETS_USER_TZ = Bytes.toBytes("tz");	//tweets.user.timezone (string)
	public static final byte[] TWEETS_USER_VERIFIED = Bytes.toBytes("verified");	//tweets.user.verified (boolean)

	public TweetsTable(HTablePool pool){
		this.pool = pool;
	}
	public long readTweetsFromDirectory(String fileDirectory,int numberOfFiles){
		int i;
		boolean isEnglish;
		Pattern pattern = Pattern.compile("\"lang\":\"([a-z][a-z])\"");
		String readFileName;
		long numberOfTweets = 0;
		long numberOfEnglishTweets = 0;
		String tweet;
		Status status;
		HTableInterface table;
		BufferedReader br = null;
		FileInputStream fis = null;
		InputStreamReader isr = null;
		try{
			table = pool.getTable(this.TWEETS);
			for(i = 0;i < numberOfFiles;i++){
				readFileName = fileDirectory + "/" + "tweets_" + Integer.toString(i) + ".json";
				//open file for reading
				fis = new FileInputStream(readFileName);
				isr = new InputStreamReader(fis,"UTF-8");
				br = new BufferedReader(isr);
				while((tweet = readTweetsFromFile(br)) != null){
					numberOfTweets++;
					//check language of tweet
					isEnglish = checkLanguage(tweet,pattern);

					status = DataObjectFactory.createStatus(tweet);
					if(status != null && isEnglish == true){
						numberOfEnglishTweets++;
						Put p = makePut(status);
						table.put(p);
					}
				}	//while
				//close files
				if(br != null)
					br.close();
				if(fis != null)
					fis.close();
				if(isr != null)
					isr.close();				
			}	//for
			table.close();
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
			System.out.println("Total number of tweets:" + numberOfTweets);
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
			System.out.println("Total number of english tweets:" + numberOfEnglishTweets);
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
		}	//try
		catch(IOException e){
			e.printStackTrace();
			System.out.println("IOException: " + e.getMessage());
		}
		catch(Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e.getMessage());
		}
		//		finally{
		//			try{
		//				if(br != null)
		//					br.close();
		//				if(fis != null)
		//					fis.close();
		//				if(isr != null)
		//					isr.close();
		//			}
		//			catch(IOException e){
		//				e.printStackTrace();
		//				System.out.println("IOException: " + e.getMessage());
		//			}
		//		}
		return numberOfEnglishTweets;
	}

	private boolean checkLanguage(String tweet,Pattern pattern) {
		//Its not necessary that user's language and tweet's text language are both english. user's lang could be en yet the
		//lang of text could be hu
		boolean isEnglish = true;
		Matcher m = pattern.matcher(tweet);
		while(m.find()){
			String lang = m.group();
			if(!lang.substring(lang.indexOf(":") + 1).equals("\"en\"")){
				isEnglish = false;
				break;
			}
		}
		return isEnglish;
	}
	private String readTweetsFromFile(BufferedReader br) throws IOException{
		StringBuilder sb = new StringBuilder();
		String line;
		while((line = br.readLine()) != null){
			if(line.equals(delimiter)){
				break;
			}
			else{
				sb.append(line);
				sb.append("\n");
			}
		}
		if(sb.length() > 0){
			sb.deleteCharAt(sb.length() - 1);
			return sb.toString();
		}
		else{
			return null;
		}
	}

	public void InitTweetsTable() throws Exception{
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
		if(hbaseAdmin.tableExists(this.TWEETS)){
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
			System.out.println("tweets table already exists!");
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
		}
		else{
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
			System.out.println("Creating table with name tweets");
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
			HTableDescriptor tableDesc = new HTableDescriptor(this.TWEETS);
			HColumnDescriptor familyDesc;
			for(String family : this.FAMILIES){
				familyDesc = new HColumnDescriptor(Bytes.toBytes(family));
				familyDesc.setMaxVersions(1);
				tableDesc.addFamily(familyDesc);
			}
			hbaseAdmin.createTable(tableDesc);
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
			System.out.println("Table with name tweets created!");
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
		}
	}

	public void deleteTweetsTable() throws Exception{
		char firstResponse;
		//Scanner s = new Scanner(System.in);
		System.out.println("Do you want to delete table with name tweets");
		System.out.println("Enter y or n:");
		//firstResponse = (char) s.nextByte();
		firstResponse = (char) System.in.read();
		//System.out.println("Confirm deletion again,Enter y or n:");
		//secondResponse = (char) s.nextByte();
		if(firstResponse == 'y'){
			Configuration conf = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(conf);
			if(admin.tableExists(this.TWEETS)){
				System.out.println("### ### ### ### ### ### ### ### ### ### ###");
				System.out.println("Deleting tweets table!");
				System.out.println("### ### ### ### ### ### ### ### ### ### ###");
				if(admin.isTableEnabled(this.TWEETS)){
					admin.disableTable(this.TWEETS);	
				}
				admin.deleteTable(this.TWEETS);
				System.out.println("### ### ### ### ### ### ### ### ### ### ###");
				System.out.println("tweets table deleted!");
				System.out.println("### ### ### ### ### ### ### ### ### ### ###");
			}
			else{
				System.out.println("### ### ### ### ### ### ### ### ### ### ###");
				System.out.println("tweets table does not exist!");
				System.out.println("### ### ### ### ### ### ### ### ### ### ###");
			}
		}
		else{
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
			System.out.println("Deletion of table with name tweets failed!");
			System.out.println("### ### ### ### ### ### ### ### ### ### ###");
		}		
	}
	private Put makePut(Status status) {
		//status is not null for sure
		Put p = new Put(Bytes.toBytes(status.getId()));
		//add to info
		Date date = status.getCreatedAt();
		if(date != null){
			p.add(TWEETS_INFO, TWEETS_INFO_CREATED,Bytes.toBytes(date.toString()));		    	
		}
		p.add(TWEETS_INFO, TWEETS_INFO_TEXT, Bytes.toBytes(status.getText()));
		p.add(TWEETS_INFO, TWEETS_INFO_SENSITIVE, Bytes.toBytes(status.isPossiblySensitive()));
		p.add(TWEETS_INFO, TWEETS_INFO_RTC, Bytes.toBytes(status.getRetweetCount()));
		//add to place
		Place place = status.getPlace();
		if(place != null){
			if(place.getName() != null){
				p.add(TWEETS_PLACE, TWEETS_PLACE_NAME, Bytes.toBytes(place.getName()));				
			}
			if(place.getCountry() != null){
				p.add(TWEETS_PLACE, TWEETS_PLACE_COUNTRY, Bytes.toBytes(place.getCountry()));				
			}
		}
		//add to user
		User user = status.getUser();
		if(user != null){
			p.add(TWEETS_USER, TWEETS_USER_ID, Bytes.toBytes(user.getId()));
			if(user.getLang() != null){
				p.add(TWEETS_USER, TWEETS_USER_LANG, Bytes.toBytes(user.getLang()));
			}
			if(user.getLocation() != null){
				p.add(TWEETS_USER, TWEETS_USER_LOC,Bytes.toBytes(user.getLocation()));				
			}
			if(user.getName() != null){
				p.add(TWEETS_USER, TWEETS_USER_NAME, Bytes.toBytes(user.getName()));				
			}
			if(user.getScreenName() != null){
				p.add(TWEETS_USER, TWEETS_USER_SNAME, Bytes.toBytes(user.getScreenName()));				
			}
			if(user.getTimeZone() != null){
				p.add(TWEETS_USER, TWEETS_USER_TZ, Bytes.toBytes(user.getTimeZone()));				
			}
			if(user.getURL() != null){
				p.add(TWEETS_USER, TWEETS_USER_URL, Bytes.toBytes(user.getURL()));				
			}
			p.add(TWEETS_USER, TWEETS_USER_VERIFIED, Bytes.toBytes(user.isVerified()));		    	
		}
		//add geolocation
		GeoLocation geo = status.getGeoLocation();
		if(geo != null){
			p.add(TWEETS_GEO, TWEETS_GEO_LAT, Bytes.toBytes(geo.getLatitude()));
			p.add(TWEETS_GEO, TWEETS_GEO_LON, Bytes.toBytes(geo.getLongitude()));		    	
		}

		StringBuilder sb = new StringBuilder();

		URLEntity[] urls = status.getURLEntities();
		if(urls != null){
			for(URLEntity url : urls){
				sb.append(url.getURL()).append(",");
			}
			if(sb.length() > 0){
				sb.deleteCharAt(sb.length() - 1);
				p.add(TWEETS_ENTITY, TWEETS_ENTITY_URLS,Bytes.toBytes(sb.toString()));		    				    	
			}
		}

		sb.delete(0,sb.length());

		HashtagEntity[] tags = status.getHashtagEntities();
		if(tags != null){
			for(HashtagEntity tag : tags){
				sb.append(tag.getText()).append(",");
			}
			if(sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
				p.add(TWEETS_ENTITY, TWEETS_ENTITY_HTAGS, Bytes.toBytes(sb.toString()));
			}
		}
		return p;
	}
}