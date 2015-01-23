package de.kp.insight.profile
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

class ProfilerService(val appName:String) extends SparkService {
  
  protected val sc = createCtxLocal("ProfileContext",Configuration.spark)      
  protected val system = ActorSystem("ProfileSystem")

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
    
    val uid = parser.option[String](List("uid"),"uid","Unique job identifier")
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
  
    val jobs = List("CPP")
    if (jobs.contains(job.value.get) == false)
      throw new Exception("Job parameter must be one of [CPP].")
 
    /* Collect parameters */
    val params = HashMap.empty[String,String]
     
    params += "site" -> site.value.get
     
    params += "uid" -> uid.value.get
    params += "job" -> job.value.get
    
    params += "customer" -> customer.value.getOrElse(0).toString
    params += "timestamp" -> new DateTime().getMillis.toString

    params.toMap
    
  }
  
  protected def initialize(params:Map[String,String]) {
    registerESTask(params)
  }

  private def registerESTask(params:Map[String,String]) = {
    
    val key = "PROFILE:" + params(Names.REQ_NAME) + ":" + params(Names.REQ_UID)
    val task = "Profile purchase data with " + appName + "."
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
  
}