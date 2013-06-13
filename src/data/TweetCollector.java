package data;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
//import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

public class TweetCollector {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length == 0){
			System.out.println("Not enought arguments");
			System.out.println("Usage: TweetCollector ##flat-file-directory##");
			System.exit(-1);
		}
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setJSONStoreEnabled(true);
        cb.setOAuthConsumerKey("XXXXXXXXXXXXXXXXXXXXXXX");
        cb.setOAuthConsumerSecret("XXXXXXXXXXXXXXXXXXXXXXXXXXX");
        cb.setOAuthAccessToken("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
        cb.setOAuthAccessTokenSecret("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        TweetListener listener = new TweetListener(args[0]);
        
        FilterQuery fq = new FilterQuery();
    
        String keywords[] = {"ios 7","os x","os x 10.9","wwdc 2013","wwdc2013","wwdc","wwdc 13"};
        
        System.out.println("Filter keywords:");
        for(String keyword : keywords){
        	System.out.print(keyword + ",");
        }
        System.out.println();
        
        fq.track(keywords);

        twitterStream.addListener(listener);
        twitterStream.filter(fq);  
	}
}

class TweetListener implements StatusListener{
	BufferedWriter bw = null;
	FileOutputStream fos = null;
    OutputStreamWriter osw = null;
    final String delimiter = "### ### ### ### ###";
	int numberOfFiles;
	long numberOfTweets;
	String fileDirectory;
	String currentFileName;
	long maxTweetsPerFile;
	public TweetListener(String fileDirectory){
		this.maxTweetsPerFile = 20000;
		this.numberOfFiles = 0;
		this.numberOfTweets = this.maxTweetsPerFile;
		this.fileDirectory = fileDirectory; 
	}
	@Override
	public void onException(Exception e) {
    	System.out.println("Exception message:" + e.getMessage());
	}

	@Override
    public void onDeletionNotice(StatusDeletionNotice notice) {
    	System.out.println("DeletionNotice received for user id:" + notice.getUserId() + " and status id:" + notice.getStatusId());
    }

	@Override
    public void onScrubGeo(long userId, long upToStatusId) {
    	System.out.println("Geolocation deletion notice received for user id:" + userId);
    }

	@Override
	public void onStallWarning(StallWarning warning) {
		System.out.println("Received stall warning:" + warning.getMessage());
	}

	@Override
    public void onStatus(Status status) {
    	try{
    		if(numberOfTweets < this.maxTweetsPerFile){
    			//bw.write(this.getFormattedStatus(status));
    			bw.write(DataObjectFactory.getRawJSON(status));
    			bw.write("\n");
    			bw.write(this.delimiter);
    			bw.write("\n");
    			numberOfTweets++;
    			if(numberOfTweets % 1000 == 0){
    				System.out.print(" - ");
    			}
    		}
    		else{
    			if(bw != null){
    				bw.close();	
    			}
    			this.currentFileName = this.fileDirectory + "/" + "tweets_" + Integer.toString(numberOfFiles) + ".json";
    			fos = new FileOutputStream(this.currentFileName);
                osw = new OutputStreamWriter(fos, "UTF-8");
                bw = new BufferedWriter(osw);
    			this.numberOfFiles++;
    			this.numberOfTweets = 0;
    			System.out.println();
    			System.out.println("number of files:" + this.numberOfFiles);
    		}
    	}
    	catch(Exception e){
    		System.out.println("Exception: " + e.getMessage());
    	}
    }

	@Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		System.out.println("Stream is becoming unlimited,need to reduce number of keywords");
		System.out.println("numberOfLimitedStatuses:" + numberOfLimitedStatuses);
	}
}