package com.streaming

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import twitter4j.Status

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 1 minute window.
 */
object PopularHashtags {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val streamingContext: StreamingContext = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val statuses: ReceiverInputDStream[Status] = TwitterUtils.createStream(streamingContext, None)
    
    // Now extract the text of each status update into DStreams using map()
    val tweets: DStream[String] = statuses.map(status => status.getText())
    
    // Blow out each word into a new DStream
    val tweetwords: DStream[String] = tweets.flatMap(tweetText => tweetText.split(" "))
    
    // Now eliminate anything that's not a hashtag
    val hashtags: DStream[String] = tweetwords.filter(word => word.startsWith("#"))

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues: DStream[(String, Int)] = hashtags.map(hashtag => (hashtag, 1))
    
    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts: DStream[(String, Int)] = hashtagKeyValues.reduceByKeyAndWindow((x:Int, y:Int) => x + y, Seconds(60), Seconds(1))

    // Sort the results by the count values
    val sortedResults: DStream[(String, Int)] = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
    
    // Print the top 10
    sortedResults.print
    
    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    streamingContext.checkpoint("C:/checkpoint/")
    streamingContext.start()
    streamingContext.awaitTermination()
  }  
}
