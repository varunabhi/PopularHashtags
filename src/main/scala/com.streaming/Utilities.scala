package com.streaming

object Utilities {
    /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter(): Unit = {
    import scala.io.Source
    
    for (line <- Source.fromFile("C:\\Streaming\\TwitterData\\src\\main\\resources\\twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
}