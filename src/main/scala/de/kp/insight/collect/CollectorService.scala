package de.kp.insight.collect
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

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import org.clapper.argot._

import de.kp.spark.core.Names
import de.kp.spark.core.SparkService

import de.kp.insight._
import org.elasticsearch.common.xcontent.XContentFactory

import scala.collection.mutable.{Buffer,HashMap}

class CollectorService extends SparkService {
  
  protected val sc = createCtxLocal("CollectContext",Configuration.spark)      
  protected val system = ActorSystem("CollectSystem")

  protected val inbox = Inbox.create(system)
  
  sys.addShutdownHook({
    /*
     * In case of a system shutdown, we also make clear
     * that the SparkContext is properly stopped as well
     * as the respective Akka actor system
     */
    sc.stop
    system.shutdown
    
  })
  
  protected val ctx = new RequestContext(sc)
  
  protected def createParams(args:Array[String]):Map[String,String] = {

    import ArgotConverters._
     
    val parser = new ArgotParser(
      programName = "Collector",
      compactUsage = true,
      preUsage = Some("Version %s. Copyright (c) 2015, %s.".format("1.0","Dr. Krusche & Partner PartG"))
    )

    val site = parser.option[String](List("key"),"key","Unique application key")
    
    val uid = parser.option[String](List("uid"),"uid","Unique job identifier")
    val job = parser.option[String](List("job"),"job","Unique job descriptor")

    val created_at_min = parser.option[String](List("min_date"),"created_at_min","Store data created after this date.")
    val created_at_max = parser.option[String](List("max_date"),"created_at_max","Store data created before this date.")

    parser.parse(args)
    
    /* Validate parameters */
    if (site.hasValue == false)
      throw new Exception("Parameter 'key' is missing.")

    if (uid.hasValue == false)
      throw new Exception("Parameter 'uid' is missing.")
    
    if (job.hasValue == false)
      throw new Exception("Parameter 'job' is missing.")
      
    if (created_at_min.hasValue == false)
      throw new Exception("Parameter 'min_date' is missing.")
      
    if (created_at_max.hasValue == false)
      throw new Exception("Parameter 'max_date' is missing.")
  
    val jobs = List("ALL","CSM","ORD","PRD")
    if (jobs.contains(job.value.get) == false)
      throw new Exception("Job parameter must be one of [ALL, CSM, ORD, PRD].")
 
    /* Collect parameters */
    val params = HashMap.empty[String,String]
     
    params += "site" -> site.value.get

    params += "uid" -> uid.value.get
    params += "job" -> job.value.get
      
    params += "created_at_min" -> created_at_min.value.get
    params += "created_at_max" -> created_at_max.value.get
    
    params += "timestamp" -> new DateTime().getMillis.toString

    params.toMap
    
  }
  
  protected def initialize(params:Map[String,String]) {
    /*
     * Create Elasticsearch databases and register 
     * the respective task in the task database
     */
    createESIndexes(params)
    registerESTask(params)
    
  }

  private def registerESTask(params:Map[String,String]) = {
    
    val key = "COLLECT:" + params(Names.REQ_NAME) + ":" + params(Names.REQ_UID)
    val task = "Data collection tasks."
    /*
     * Note, that we do not specify additional
     * payload data here
     */
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	
	/* key */
	builder.field("key",key)
	
	/* task */
	builder.field("task",task)
	
	/* timestamp */
	builder.field("timestamp",params("timestamp").toLong)
	
	builder.endObject()
	/*
	 * Register data in the 'admin/tasks' index
	 */
	ctx.putSource("admin","tasks",builder)

  }

  private def createESIndexes(params:Map[String,String]) {
    
    val uid = params(Names.REQ_UID)
    /*
     * Create search indexes (if not already present)
     * 
     * The 'customer' index (mapping) specifies a customer database that
     * holds synchronized customer data relevant for the insight server
     * 
     * The 'product' index (mapping) specifies a product database that
     * holds synchronized product data relevant for the insight server
     * 
     * The 'order' index (mapping) specifies an order database that
     * holds synchronized order data relevant for the insight server
     */
    
    if (ctx.createIndex("customers","base","CSM") == false)
      throw new Exception("Index creation for 'customers/base' has been stopped due to an internal error.")
 
    if (ctx.createIndex("products","base","PRD") == false)
      throw new Exception("Index creation for 'products/base' has been stopped due to an internal error.")
 
    if (ctx.createIndex("orders","base","ORD") == false)
      throw new Exception("Index creation for 'orders/base' has been stopped due to an internal error.")
    
  }
  
  private def unformatted(date:String):Long = {

    //2008-12-31 03:00
    val pattern = "yyyy-MM-dd HH:mm"
    val formatter = DateTimeFormat.forPattern(pattern)
 
    formatter.parseMillis(date)
    
  }
  
}