import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class ParallelSprout extends BaseRichSpout {

    SpoutOutputCollector collector;
    LinkedBlockingQueue<String> queue = null;
    TwitterStream twitterStream;
    private FileWriter tweetsfile;
    private BufferedWriter tweetsbuffer;




    String CONSUMER_KEY = "KJE7Fv87Nu63YwrR0mMvGSP7C";
    String CONSUMER_SECRET_KEY = "RD69xokfikdN8yR5dHQJNw0ODWmE0XIkbcxeRiIGZcNnwkJebS";
    String ACCESS_TOKEN ="1367899124381540352-IkwYhUACAZl3NJRm3XwSUgCdsXGJLF";
    String ACCESS_TOKEN_SECRET = "A6F9a2Bq1TyXZz2hbIpSOG9raFFixcQrBUtdC03tZx9dw";


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.queue = new LinkedBlockingQueue<String>(1000);
        this.collector = collector;
        try {
            tweetsfile = new FileWriter( "/s/chopin/a/grad/mohitkum/parallel.txt", true);
            tweetsbuffer = new BufferedWriter(tweetsfile);
        } catch (Exception e) {
            System.out.println("Error in writing to file 1 ");
            e.printStackTrace();
        }

        StatusListener statusListener = new StatusListener() {

            public void onStatus(Status status) {
                try {

                    tweetsbuffer.write(status.getText());
                    tweetsbuffer.write("\n");
                    tweetsbuffer.flush();
                    for (HashtagEntity ht : status.getHashtagEntities()) {

                        String hashTag = ht.getText().toLowerCase();

                        if (!hashTag.isEmpty()) {
                            try {
                                ParallelSprout.this.queue.put(hashTag);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            Utils.sleep(50);
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            public void onTrackLimitationNotice(int i) {
            }

            public void onScrubGeo(long l, long l1) {
            }

            public void onException(Exception ex) {
            }

            public void onStallWarning(StallWarning arg0) {

            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(CONSUMER_KEY)
                .setOAuthConsumerSecret(CONSUMER_SECRET_KEY)
                .setOAuthAccessToken(ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        FilterQuery tweetFilterQuery = new FilterQuery().language("en");
        twitterStream.addListener(statusListener);
        twitterStream.filter(tweetFilterQuery);
        twitterStream.sample();

    }

    public void nextTuple() {
        String ret = this.queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            this.collector.emit(new Values(ret));
        }
    }

    public void close() {
        twitterStream.shutdown();
    }

    public void ack(Object id) {
    }

    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashTag"));
    }

}