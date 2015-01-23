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

import org.apache.spark.rdd.RDD
import akka.actor._

import de.kp.spark.core.Names

import de.kp.insight.RequestContext
import de.kp.insight.model._

import scala.concurrent.duration.DurationInt
import scala.collection.mutable.HashMap

object PreparerApp extends PreparerService("Preparer") {
  
  def main(args:Array[String]) {

    try {

      /*
       * Add internal arguments to request parameters; one of
       * these arguments is the name of the respective task
       */
      val params = createParams(args)
      
      val job = params("job")
      val customer = params("customer")
      
      val req_params = (
        if (List("LOC","POM","RFM").contains("job")) {
          /*
           * The job descriptor is directly used as the name designator
           */
          params ++ Map(Names.REQ_NAME -> job) 
        
        } else {
          /*
           * The job descriptor is extended with the customer (type) 
           * provided to form the name designator for this task
           * 
           */
          params ++ Map(Names.REQ_NAME -> String.format("""%s-%s""",job,customer))
        }
        
      )
      /*
       * Load orders from Elasticsearch order database and start handler
       * to extract & prepare data from the different purchase transactions 
       * and store the result as a Parquet file
       */
      val orders = initialize(req_params)
      /*
       * Start & monitor PreparerActor
       */
      val actor = system.actorOf(Props(new Handler(ctx,orders)))   
      inbox.watch(actor)
    
      actor ! StartPrepare(req_params)

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

  class Handler(ctx:RequestContext,orders:RDD[InsightOrder]) extends Actor {
    
    override def receive = {
    
      case msg:StartPrepare => {

        val start = new java.util.Date().getTime     
        println("Preparer started at " + start)
 
        val job = msg.data("job")        
        val preparer = job match {
          
          case "CDA" => context.actorOf(Props(new CDAPreparer(ctx,orders))) 
          
          case "CHA" => context.actorOf(Props(new CHAPreparer(ctx,orders))) 
          
          case "CLS" => context.actorOf(Props(new CLSPreparer(ctx,orders))) 
          
          case "CPA" => context.actorOf(Props(new CPAPreparer(ctx,orders))) 

          case "CPS" => context.actorOf(Props(new CPSPreparer(ctx,orders))) 
          
          case "CSA" => context.actorOf(Props(new CSAPreparer(ctx,orders))) 
          
          case "DPS" => context.actorOf(Props(new DPSPreparer(ctx,orders))) 
          
          case "LOC" => context.actorOf(Props(new LOCPreparer(ctx,orders))) 
          
          case "POM" => context.actorOf(Props(new POMPreparer(ctx,orders))) 
          
          case "PPF" => context.actorOf(Props(new PPFPreparer(ctx,orders))) 
          
          case "PRM" => context.actorOf(Props(new PRMPreparer(ctx,orders))) 

          case "RFM" => context.actorOf(Props(new RFMPreparer(ctx,orders))) 
          
          case _ => throw new Exception("Wrong job descriptor.")
          
        }
        
        preparer ! StartPrepare(msg.data)
       
      }
    
      case msg:PrepareFailed => {
    
        val end = new java.util.Date().getTime           
        println("Preparer failed at " + end)
    
        context.stop(self)
      
      }
    
      case msg:PrepareFinished => {
    
        val end = new java.util.Date().getTime           
        println("Preparer finished at " + end)
    
        context.stop(self)
    
      }
    
    }
  
  }
  
}