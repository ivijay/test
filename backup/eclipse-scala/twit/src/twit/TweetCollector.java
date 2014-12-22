package twit;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

public class TweetCollector {
	static int stop = 0;
	static int connect = 0;

	// Database credentials

	public static void main(String[] args) {
		System.setProperty("http.proxyHost", "172.20.181.136");
		System.setProperty("http.proxyPort", "8080");
		if (args.length < 5) {
			System.err.println("Required paramaters are missing");
		}

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setJSONStoreEnabled(true);
		//		cb.setOAuthConsumerKey("oGq2K5T2rKaQVsW60hYgrIMpB");
		//		cb.setOAuthConsumerSecret("x29V9W86cHcliTBwjvpY69lWfBrukSUIajojoq1RX1NFDcc8ST");
		//		cb.setOAuthAccessToken("86880164-GgmhOq45Jh6FigIovSClepCD9ZXO44ptIeqE5IP49");
		//		cb.setOAuthAccessTokenSecret("4mWWhnT3o86f7DIclcq25SdmtECb5DrZcrj5dFZkqAmVQ");

		cb.setOAuthConsumerKey(args[0]);
		cb.setOAuthConsumerSecret(args[1]);
		cb.setOAuthAccessToken(args[2]);
		cb.setOAuthAccessTokenSecret(args[3]);

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
		.getInstance();

		StatusListener listener = new StatusListener() {

			@Override
			public void onException(Exception arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onStatus(Status status) {
				//                User user = status.getUser();
				//
				//                String username = status.getUser().getScreenName();
				//                System.out.println(username);
				//                String profileLocation = user.getLocation();
				//                System.out.println(profileLocation);
				//                long tweetId = status.getId(); 
				//                System.out.println(tweetId);
				//                String content = status.getText();
				//                System.out.println(content +"\n");

				String rawstatus = DataObjectFactory.getRawJSON(status);
				String fileName = "/home/HadoopUser/Downloads/target/" + status.getId() + ".json";
				//				System.out.println("rawstatus: " + rawstatus);
				try {
					storeJSON(rawstatus, fileName);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

			@Override
			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		// More robust filtering like by location/time zone/etc here itself?
		FilterQuery fq = new FilterQuery();

		String keywords[] = {"test"};

		for(int i=4;i<=(args.length-1);i++) {
			keywords[i-4] = args[i];
		}

		System.out.print("monitoring for the keywords: ");
		for(String s: keywords) { 
			System.out.print(s + "\t");
		}
		System.out.println();

		fq.track(keywords);

		twitterStream.addListener(listener);
		twitterStream.filter(fq);

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

			System.out.println("in Null pointer Exception:" + rawJSON);
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