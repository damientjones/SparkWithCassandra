/**
 * Created by damien on 9/12/2015.
 */

package Util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.log4j.{Level, Logger}

object CreateSparkContext {

  private var sc : SparkContext = _
  private var conf : SparkConf = _
  private var csc : CassandraSQLContext = _

  def CreateContext (url:String,userName:String,password:String,appName:String,master:String) {
  //defaults are 127.0.0.1, cassandra, cassandra, testApp, local[2]
    if (sc == null){
      val level = Level.ERROR
      Logger.getLogger("org").setLevel(level)
      Logger.getLogger("akka").setLevel(level)
      conf = new SparkConf(true)
        .setAppName(appName)
        .setMaster(master)
        .set("spark.cassandra.connection.host",url)
        .set("spark.cassandra.auth.username",userName)
        .set("spark.cassandra.auth.password",password)
      sc = new SparkContext(conf)
      csc = new CassandraSQLContext(sc)
    }
  }
  def getCassandraContext : CassandraSQLContext = {
    csc
  }
  def closeSparkContext {
    sc.stop()
  }
}
