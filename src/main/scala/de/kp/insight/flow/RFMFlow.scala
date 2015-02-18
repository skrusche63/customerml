package de.kp.insight.flow
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Shopify-Insight project
* (https://github.com/skrusche63/shopify-insight).
* 
* Shopify-Insight is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Shopify-Insight is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Shopify-Insight. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import akka.actor._

import org.apache.spark.rdd.RDD

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import org.clapper.argot._

import de.kp.spark.core.Names
import de.kp.spark.core.SparkService

import de.kp.insight._
import de.kp.insight.model._

import de.kp.insight.prepare.RFMPreparer
import de.kp.insight.storage.RFMLoader

import de.kp.insight.util.ElasticRDD

import org.elasticsearch.index.query._
import org.elasticsearch.common.xcontent.XContentFactory

import scala.collection.mutable.{Buffer,HashMap}
import scala.concurrent.duration.DurationInt

object RFMFlow extends SparkService {
  
  private val sc = createCtxLocal("FlowContext",Configuration.spark)      
  private val system = ActorSystem("FlowSystem")

  private val inbox = Inbox.create(system)
  
  sys.addShutdownHook({
    /*
     * In case of a system shutdown, we also make clear
     * that the SparkContext is properly stopped as well
     * as the respective Akka actor system
     */
    sc.stop
    system.shutdown
    
  })
  
  private val ctx = new RequestContext(sc)
  
  def main(args:Array[String]) {

    try {

      val params = createParams(args)      
    
      if (ctx.createIndex("customers","segments","RFM") == false)
        throw new Exception("Index creation for 'customers/segments' has been stopped due to an internal error.")

      val orders = getOrders(params)

      val actor = system.actorOf(Props(new Handler(ctx,params,orders)))   
      inbox.watch(actor)
    
      actor ! StartPrepare

      val timeout = DurationInt(30).minute
    
      while (inbox.receive(timeout).isInstanceOf[Terminated] == false) {}    
      sys.exit
      
    } catch {
      case e:Exception => {
          
        println(e.getMessage) 
        sys.exit
          
      }
    
    }

  }
  
  class Handler(ctx:RequestContext,params:Map[String,String],orders:RDD[InsightOrder]) extends Actor {
    
    override def receive = {
    
      case msg:StartPrepare => {

        val start = new java.util.Date().getTime     
        println("Preparer started at " + start)
 
        registerPrepare(params)        
        val preparer = context.actorOf(Props(new RFMPreparer(ctx,params,orders))) 
        
        preparer ! StartPrepare
       
      }
    
      case msg:PrepareFailed => {
    
        val end = new java.util.Date().getTime           
        println("Preparer failed at " + end)
    
        context.stop(self)
      
      }
    
      case msg:PrepareFinished => {
    
        val end = new java.util.Date().getTime           
        println("Preparer finished at " + end)
    
        registerLoad(params)
        val loader = context.actorOf(Props(new RFMLoader(ctx,params))) 
        
        loader ! StartLoad
        
      }
    
      case msg:LoadFailed => {
    
        val end = new java.util.Date().getTime           
        println("Loader failed at " + end)
    
        context.stop(self)
      
      }
    
      case msg:LoadFinished => {
    
        val end = new java.util.Date().getTime           
        println("Loader finished at " + end)
    
        context.stop(self)
    
      }
    
    }
  
  }
  
  private def createParams(args:Array[String]):Map[String,String] = {

    import ArgotConverters._
     
    val parser = new ArgotParser(
      programName = "RFM Dataflow",
      compactUsage = true,
      preUsage = Some("Version %s. Copyright (c) 2015, %s.".format("1.0","Dr. Krusche & Partner PartG"))
    )

    val site = parser.option[String](List("key"),"key","Unique application key")    
    val uid = parser.option[String](List("uid"),"uid","Unique preparation identifier")
    /*
     * The subsequent parameters specify a certain period of time with 
     * a minimum and maximum date
     */
    val created_at_min = parser.option[String](List("min_date"),"created_at_min","Store data created after this date.")
    val created_at_max = parser.option[String](List("max_date"),"created_at_max","Store data created before this date.")

    parser.parse(args)
      
    /* Validate & collect parameters */
    val params = HashMap.empty[String,String]
    params += "timestamp" -> new DateTime().getMillis().toString

    site.value match {      
      case None => parser.usage("Parameter 'key' is missing.")
      case Some(value) => params += "site" -> value
    }
    
    uid.value match {      
      case None => parser.usage("Parameter 'uid' is missing.")
      case Some(value) => params += "uid" -> value
    }
    
    created_at_min.value match {      
      case None => parser.usage("Parameter 'created_at_min' is missing.")
      case Some(value) => params += "created_at_min" -> value
    }
     
    created_at_max.value match {      
      case None => parser.usage("Parameter 'created_at_max' is missing.")
      case Some(value) => params += "created_at_max" -> value
    }
      
    params += Names.REQ_NAME -> "RFM"     
    params.toMap
    
  }
  
  private def getOrders(params:Map[String,String]):RDD[InsightOrder] = {

    val esConfig = ctx.getESConfig

    esConfig.set(Names.ES_RESOURCE,("orders/base"))
    esConfig.set(Names.ES_QUERY,query(params))
      
    val elasticRDD = new ElasticRDD(ctx.sparkContext)
         
    val rawset = elasticRDD.read(esConfig)
    elasticRDD.orders(rawset)
    
  }
  /**
   * This method registers the data preparation task in the respective
   * Elasticsearch index; this information supports administrative
   * tasks such as the monitoring of this insight server
   */
  private def registerPrepare(params:Map[String,String]) = {
    
    val key = "PREPARE:" + params(Names.REQ_NAME) + ":" + params(Names.REQ_UID)
    val task = "Data preparation for RFM Data flow."
      
    val timestamp = params("timestamp").toLong
    register(key,task,timestamp)

  }
  /**
   * This method registers the data storage task in the respective
   * Elasticsearch index; this information supports administrative
   * tasks such as the monitoring of this insight server
   */
  private def registerLoad(params:Map[String,String]) = {
    
    val key = "LOAD:" + params(Names.REQ_NAME) + ":" + params(Names.REQ_UID)
    val task = "Data storage for RFM Data flow."
      
    val timestamp = params("timestamp").toLong
    register(key,task,timestamp)

  }
  
  private def register(key:String,task:String,timestamp:Long) {

    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	
	/* key */
	builder.field("key",key)
	
	/* task */
	builder.field("task",task)
	
	/* timestamp */
	builder.field("timestamp",timestamp)
	
	builder.endObject()
	/*
	 * Register data in the 'admin/tasks' index
	 */
	ctx.putSource("admin","tasks",builder)
    
  }
  
  private def query(params:Map[String,String]):String = {
    
    val created_at_min = unformatted(params("created_at_min"))
    val created_at_max = unformatted(params("created_at_max"))
            
    val filters = Buffer.empty[FilterBuilder]
    filters += FilterBuilders.rangeFilter("time").from(created_at_min).to(created_at_max)
    
    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)
    
    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    qbuilder.toString
    
  }
  
  private def unformatted(date:String):Long = {

    //2008-12-31 03:00
    val pattern = "yyyy-MM-dd HH:mm"
    val formatter = DateTimeFormat.forPattern(pattern)
 
    formatter.parseMillis(date)
    
  }
}