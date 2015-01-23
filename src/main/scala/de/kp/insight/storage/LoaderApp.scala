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

import org.apache.spark.rdd.RDD
import akka.actor._

import de.kp.spark.core.Names

import de.kp.insight.RequestContext
import de.kp.insight.model._

import scala.concurrent.duration.DurationInt
import scala.collection.mutable.HashMap

object LoaderApp extends LoaderService("Loader") {
  
  def main(args:Array[String]) {

    try {

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
 
      initialize(req_params)

      val actor = system.actorOf(Props(new Handler(ctx,req_params)))   
      inbox.watch(actor)
    
      actor ! StartLoad

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

  class Handler(ctx:RequestContext,params:Map[String,String]) extends Actor {
    
    override def receive = {
    
      case msg:StartLoad => {

        val start = new java.util.Date().getTime     
        println("Loader started at " + start)
 
        val job = params("job")        
        val loader = job match {
          
          case "CCS" => context.actorOf(Props(new CCSLoader(ctx,params))) 
          
          case "CDA" => context.actorOf(Props(new CDALoader(ctx,params))) 
          
          case "CHA" => context.actorOf(Props(new CHALoader(ctx,params))) 
          
          case "CLS" => context.actorOf(Props(new CLSLoader(ctx,params))) 
          
          case "CPA" => context.actorOf(Props(new CPALoader(ctx,params))) 
          
          case "CPF" => context.actorOf(Props(new CPFLoader(ctx,params))) 
          
          case "CPP" => context.actorOf(Props(new CPPLoader(ctx,params))) 
          
          case "CPR" => context.actorOf(Props(new CPRLoader(ctx,params))) 
          
          case "CSA" => context.actorOf(Props(new CSALoader(ctx,params))) 
          
          case "DPS" => context.actorOf(Props(new DPSLoader(ctx,params))) 
          
          case "LOC" => context.actorOf(Props(new LOCLoader(ctx,params))) 
          
          case "PCR" => context.actorOf(Props(new PCRLoader(ctx,params))) 
          
          case "POM" => context.actorOf(Props(new POMLoader(ctx,params))) 
          
          case "PPF" => context.actorOf(Props(new PPFLoader(ctx,params))) 
          
          case "PRM" => context.actorOf(Props(new PRMLoader(ctx,params))) 
          
          case "RFM" => context.actorOf(Props(new RFMLoader(ctx,params))) 
          
          case _ => throw new Exception("Wrong job descriptor.")
          
        }
        
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
  
}