package de.kp.insight.predict
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

object PredictorApp extends PredictorService("Predictor") {
  
  def main(args:Array[String]) {

    try {

      val params = createParams(args)
      
      val job = params("job")
      val customer = params("customer")
      
      val req_params = params ++ Map(Names.REQ_NAME -> String.format("""%s-%s""",job,customer))
      initialize(req_params)

      val actor = system.actorOf(Props(new Handler(ctx,req_params)))   
      inbox.watch(actor)
    
      actor ! StartPredict

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
    
      case msg:StartPredict => {

        val start = new java.util.Date().getTime     
        println("Predictor started at " + start)
 
        val job = params("job")        
        val loader = job match {
          
          case "RFM" => context.actorOf(Props(new RFMPredictor(ctx,params))) 
          
          case _ => throw new Exception("Wrong job descriptor.")
          
        }
        
        loader ! StartPredict
       
      }
    
      case msg:PredictFailed => {
    
        val end = new java.util.Date().getTime           
        println("Predictor failed at " + end)
    
        context.stop(self)
      
      }
    
      case msg:PredictFinished => {
    
        val end = new java.util.Date().getTime           
        println("Predict finished at " + end)
    
        context.stop(self)
    
      }
    
    }
  
  }
  
}