package de.kp.insight.storage
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
import org.elasticsearch.common.xcontent.XContentFactory

import scala.collection.mutable.{Buffer,HashMap}
import de.kp.insight.Configuration

class LoaderService(val appName:String) extends SparkService {
  
  protected val sc = createCtxLocal("LoaderContext",Configuration.spark)      
  protected val system = ActorSystem("LoaderSystem")

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
      programName = appName,
      compactUsage = true,
      preUsage = Some("Version %s. Copyright (c) 2015, %s.".format("1.0","Dr. Krusche & Partner PartG"))
    )

    val site = parser.option[String](List("key"),"key","Unique application key")

    val uid = parser.option[String](List("uid"),"uid","Unique preparation identifier")
    val job = parser.option[String](List("job"),"job","Unique job descriptor")

    val customer = parser.option[Int](List("customer"),"customer","Customer type.")

    parser.parse(args)
      
    /* Validate parameters */
    if (site.hasValue == false)
      throw new Exception("Parameter 'key' is missing.")

    if (uid.hasValue == false)
      throw new Exception("Parameter 'uid' is missing.")
    
    if (job.hasValue == false)
      throw new Exception("Parameter 'job' is missing.")
  
    val jobs = List(
        /* customer centric */
        "CCS","CDA","CHA","CLS","CPA","CPF","CPP","CPR","CSA","DPS","LOC",
        /* product centric */
        "PCR","POM","PPF","PPS","PRM","RFM"
    )
    
    if (jobs.contains(job.value.get) == false)
      throw new Exception("Job parameter must be one of [CCS, CDA, CHA, CLS, CPF, CPA, CPP, CPR, CSA, DPS, LOC, PCR, POM, PPF, PPS, PRM, RFM].")
     
    /* Collect parameters */
    val params = HashMap.empty[String,String]
     
    params += "site" -> site.value.get
     
    params += "uid" -> uid.value.get
    params += "job" -> job.value.get
    
    params += "customer" -> customer.value.getOrElse(0).toString
    params += "timestamp" -> new DateTime().getMillis().toString
    
    params.toMap
    
  }
  
  protected def initialize(params:Map[String,String]) {
    /*
     * Create Elasticsearch task database and register 
     * the respective task in the database
     */
    createESIndexes(params)
    registerESTask(params)
    
  }
  /**
   * This method registers the data storage task in the respective
   * Elasticsearch index; this information supports administrative
   * tasks such as the monitoring of this insight server
   */
  private def registerESTask(params:Map[String,String]) = {
    
    val key = "LOAD:" + params(Names.REQ_NAME) + ":" + params(Names.REQ_UID)
    val task = "Data storage with " + appName + "."
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
    
    /*
     * Create search indexes (if not already present)
     */
 
    /********** ORDER **********/
    
    if (ctx.createIndex("orders","metrics","POM") == false)
      throw new Exception("Index creation for 'orders/metrics' has been stopped due to an internal error.")
 
    /********** PRODUCT ********/
            
    if (ctx.createIndex("products","relations","PRM") == false)
      throw new Exception("Index creation for 'products/relations' has been stopped due to an internal error.")
            
    if (ctx.createIndex("products","recommendations","PCR") == false)
      throw new Exception("Index creation for 'products/recommendations' has been stopped due to an internal error.")

    if (ctx.createIndex("products","segments","PPF") == false)
      throw new Exception("Index creation for 'products/segments' has been stopped due to an internal error.")

    if (ctx.createIndex("products","similars","PPS") == false)
      throw new Exception("Index creation for 'products/similars' has been stopped due to an internal error.")
 
    /********** CUSTOMERS ******/
    
    if (ctx.createIndex("customers","forecasts","CPF") == false)
      throw new Exception("Index creation for 'customers/forecasts' has been stopped due to an internal error.")

    if (ctx.createIndex("customers","locations","LOC") == false)
      throw new Exception("Index creation for 'customers/locations' has been stopped due to an internal error.")
            
    if (ctx.createIndex("customers","loyalties","CLS") == false)
      throw new Exception("Index creation for 'customers/loyalties' has been stopped due to an internal error.")

    if (ctx.createIndex("customers","profiles","CPP") == false)
      throw new Exception("Index creation for 'customers/profiles' has been stopped due to an internal error.")

    if (ctx.createIndex("customers","recommendations","CPR") == false)
      throw new Exception("Index creation for 'customers/recommendations' has been stopped due to an internal error.")
            
    if (ctx.createIndex("customers","segments","RFM") == false)
      throw new Exception("Index creation for 'customers/segments' has been stopped due to an internal error.")
    
    if (ctx.createIndex("customers","similars","CCS") == false)
      throw new Exception("Index creation for 'customers/similars' has been stopped due to an internal error.")
 
    /********** PERSONAS ******/
            
    if (ctx.createIndex("personas","days","CDA") == false)
      throw new Exception("Index creation for 'personas/days' has been stopped due to an internal error.")
            
    if (ctx.createIndex("personas","hours","CHA") == false)
      throw new Exception("Index creation for 'personas/hours' has been stopped due to an internal error.")
            
    if (ctx.createIndex("personas","timespans","CSA") == false)
      throw new Exception("Index creation for 'personas/timespans' has been stopped due to an internal error.")
            
    if (ctx.createIndex("personas","products","CPA") == false)
      throw new Exception("Index creation for 'personas/products' has been stopped due to an internal error.")
     
  }
  
  private def unformatted(date:String):Long = {

    //2008-12-31 03:00
    val pattern = "yyyy-MM-dd HH:mm"
    val formatter = DateTimeFormat.forPattern(pattern)
 
    formatter.parseMillis(date)
    
  }

}