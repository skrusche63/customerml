package de.kp.insight.prepare
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

import de.kp.insight.util.ElasticRDD

import org.elasticsearch.index.query._
import org.elasticsearch.common.xcontent.XContentFactory

import scala.collection.mutable.{Buffer,HashMap}
import de.kp.insight.Configuration

class PreparerService(val appName:String) extends SparkService {
  
  protected val sc = createCtxLocal("PrepareContext",Configuration.spark)      
  protected val system = ActorSystem("PrepareSystem")

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
    /*
     * The subsequent parameters specify a certain period of time with 
     * a minimum and maximum date
     */
    val created_at_min = parser.option[String](List("min_date"),"created_at_min","Store data created after this date.")
    val created_at_max = parser.option[String](List("max_date"),"created_at_max","Store data created before this date.")
    
    val customer = parser.option[Int](List("customer"),"customer","Customer type.")

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
  
    val jobs = List("CDA","CHA","CLS","CPA","CPS","CSA","DPS","LOC","POM","PPF","PRM","RFM")
    if (jobs.contains(job.value.get) == false)
      throw new Exception("Job parameter must be one of [CDA, CHA, CLS, CPA, CPS, CSA, DPS, LOC, POM, PPF, PRM, RFM].")
    
    /* Collect parameters */
    val params = HashMap.empty[String,String]
     
    params += "site" -> site.value.get

    params += "uid" -> uid.value.get
    params += "job" -> job.value.get
      
    params += "created_at_min" -> created_at_min.value.get
    params += "created_at_max" -> created_at_max.value.get
    
    params += "customer" -> customer.value.getOrElse(0).toString
    params += "timestamp" -> new DateTime().getMillis().toString
    
    params.toMap
    
  }
  
  protected def initialize(params:Map[String,String]):RDD[InsightOrder] = {
    /*
     * STEP #1: Register the respective task in the database
     */
    registerESTask(params)
    /*
     * STEP #2: Retrieve orders from the orders/base index;
     * the respective query is either provided as an external
     * parameter or computed from the period of time provided
     * with this request
     */
    val esConfig = ctx.getESConfig
    esConfig.set(Names.ES_RESOURCE,("orders/base"))

    if (params.contains(Names.REQ_QUERY))
      esConfig.set(Names.ES_QUERY,params(Names.REQ_QUERY))
      
    else 
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
  private def registerESTask(params:Map[String,String]) = {
    
    val key = "PREPARE:" + params(Names.REQ_NAME) + ":" + params(Names.REQ_UID)
    val task = "Data preparation with " + appName + "."
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