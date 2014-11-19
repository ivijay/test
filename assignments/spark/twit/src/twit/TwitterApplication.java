package twit;

import java.util.logging.Logger;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
//import twitter4j.auth.AccessToken;
//import twitter4j.auth.RequestToken;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Twitter application using Twitter4J
 */
public class TwitterApplication {

	private final Logger logger = Logger.getLogger(TwitterApplication.class.getName());

	public static void main(String[] args) {
		new TwitterApplication().retrieve();
	}

	public void retrieve() {
		System.setProperty("http.proxyHost", "172.20.181.136");
		System.setProperty("http.proxyPort", "8080");

		logger.info("Retrieving tweets...");
		ConfigurationBuilder con = new ConfigurationBuilder();
		con.setOAuthConsumerKey("oGq2K5T2rKaQVsW60hYgrIMpB");
		con.setOAuthConsumerSecret("x29V9W86cHcliTBwjvpY69lWfBrukSUIajojoq1RX1NFDcc8ST");
		con.setOAuthAccessToken("86880164-GgmhOq45Jh6FigIovSClepCD9ZXO44ptIeqE5IP49");
		con.setOAuthAccessTokenSecret("4mWWhnT3o86f7DIclcq25SdmtECb5DrZcrj5dFZkqAmVQ");
		
		con.setJSONStoreEnabled(true);
		Twitter twitter = new TwitterFactory(con.build()).getInstance();
		String user = "hadoop";
		Query query = new Query("flipkart");

		JsonWriter writer;

		//        query.setSince("2011-01-01");
		try {
			QueryResult result = twitter.search(query);
			System.out.println("Count : " + result.getTweets().size()) ;


			for (Status status : result.getTweets()) {

				System.out.println("status: " + status);
				String rawstatus = DataObjectFactory.getRawJSON(status);
				String fileName = "/home/HadoopUser/Downloads/target/" + status.getId() + ".json";
				System.out.println("rawstatus: " + rawstatus);
				storeJSON(rawstatus, fileName);

			}
		} catch (TwitterException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("done! ");
	}
	private static void storeJSON(String rawJSON, String fileName) throws IOException {
		FileOutputStream fos = null;
		OutputStreamWriter osw = null;
		BufferedWriter bw = null;
		try {
			fos = new FileOutputStream(fileName);
			osw = new OutputStreamWriter(fos, "UTF-8");
			bw = new BufferedWriter(osw);
			bw.write(rawJSON);
			bw.flush();
		} catch(NullPointerException e) {
			
			System.out.println("in NPE:" + rawJSON);
		} 
		finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException ignore) {
				}
			}
			if (osw != null) {
				try {
					osw.close();
				} catch (IOException ignore) {
				}
			}
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException ignore) {
				}
			}
		}
	}
}
