package data;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.json.DataObjectFactory;
public class FormatCollectedTweets {
	
	final static String delimiter = "### ### ### ### ###";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length < 2){
			System.out.println("Not enough arguments");
			System.out.println("Usage: FormatCollectedTweets ##Tweets File Directory## ##Number Of Files##");
			System.exit(-1);
		}
		readAndWrite(args[0],Integer.parseInt(args[1])); 		//x + 1 for tweets_x where x is largest index
	}
	
	private static void readAndWrite(String fileDirectory,int numberOfFiles){
		Pattern pattern = Pattern.compile("\"lang\":\"([a-z][a-z])\"");
		StringBuilder sb = new StringBuilder();
		BufferedReader br = null;
		BufferedWriter bw = null;
		FileInputStream fis = null;
		FileOutputStream fos = null;
		OutputStreamWriter osw = null;
		InputStreamReader isr = null;
		Status status;
		int numberOfTweets = 0;
		int numberOfEnglishTweets = 0;
		String readFileName;
		String writeFileName;
		String tweet;
		GeoLocation geo;
		boolean isEnglish;
		Place place;
		User user;
		String firstLine = "tweetId createdAt tweetGeo(latitude,longitude) tweetPlace tweetText userId userName";
		int i = 0;
		try{
			for(i = 0;i < numberOfFiles;i++){
				//file names
				readFileName = fileDirectory + "/" + "tweets_" + Integer.toString(i) + ".json";
				writeFileName = fileDirectory + "/" + "parsed_" + Integer.toString(i) + ".txt";
				//open file for reading
				fis = new FileInputStream(readFileName);
				isr = new InputStreamReader(fis,"UTF-8");
				br = new BufferedReader(isr);
				if(fis == null || isr == null || br == null){
					System.out.println("unable to read tweets");
				}
				//open file for writing
				fos = new FileOutputStream(writeFileName);
				osw = new OutputStreamWriter(fos,"UTF-8");
				bw = new BufferedWriter(osw);
				if(fos == null || osw == null || bw == null){
					System.out.println("unable to write parsed tweets");
				}
				bw.write(firstLine);
				bw.write("\n");
				//System.out.println("After writing first line");
				while((tweet = readTweet(br)) != null){
					numberOfTweets++;
					isEnglish = checkLanguage(tweet,pattern);
					sb.delete(0,sb.length());
					status = DataObjectFactory.createStatus(tweet);
					if(status != null && isEnglish == true){
						numberOfEnglishTweets++;
						sb.append(status.getId()).append("#");
						Date createdAt = status.getCreatedAt();
						if(createdAt != null){
							sb.append(createdAt.toString()).append("#");							
						}
						else{
							sb.append("null").append("#");
						}

						geo = status.getGeoLocation();
						if(geo != null){
							sb.append(geo.getLatitude()).append(",").append(geo.getLongitude()).append("#");							
						}
						else{
							sb.append("null").append("#");
						}
						place = status.getPlace();
						if(place != null){
							sb.append(place.getFullName()).append("#");	
						}
						else{
							sb.append("null").append("#");
						}
						sb.append(status.getText()).append("#");
						user = status.getUser();
						if(user != null){
							sb.append(user.getId()).append("#").append(user.getName());							
						}
						else{
							sb.append("null").append("#").append("null");
						}
						bw.write(sb.toString());
						bw.write("\n");						
					}
				}	//while
				//close files
				if(bw != null)
					bw.close();
				if(br != null)
					br.close();
				if(fos != null)
					fos.close();
				if(fis != null)
					fis.close();
				if(osw != null)
					osw.close();
				if(isr != null)
					isr.close();				
			}	//for
			System.out.println("Total number of tweets:" + numberOfTweets);
			System.out.println("Total number of english tweets:" + numberOfEnglishTweets);
		}
		catch(IOException e){
			e.printStackTrace();
			System.out.println("IOException: " + e.getMessage());
		}
		catch(Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e.getMessage());
		}
/*
		finally{
			try{
				if(bw != null)
					bw.close();
				if(br != null)
					br.close();
				if(fos != null)
					fos.close();
				if(fis != null)
					fis.close();
				if(osw != null)
					osw.close();
				if(isr != null)
					isr.close();
			}
			catch(IOException e){
				e.printStackTrace();
				System.out.println("IOException: " + e.getMessage());
			}
		}
*/
	}
	
	private static boolean checkLanguage(String tweet,Pattern pattern) {
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
	
	private static String readTweet(BufferedReader br) throws IOException{
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
}