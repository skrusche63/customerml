package de.kp.insight.learn
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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.util.{MFModel,MFUtil}
import de.kp.insight._

import de.kp.insight.model._

class RDict(val elems:Seq[String]) extends Serializable {
 
  val lookup = elems.zipWithIndex.toMap

  val getIndex = lookup
  val getTerm = elems

  val size = elems.size

}

class ALSLearner(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {

  override def receive = {
   
    case message:StartLearn => {
      
      val req_params = params
      
      val uid = req_params(Names.REQ_UID)
      val name = req_params(Names.REQ_NAME)
      
      val start = new java.util.Date().getTime.toString            
      ctx.putLog("info",String.format("""[UID: %s] %s learning request received at %s.""",uid,name,start))
      
      try {
    
        /*
         * STEP #1: Starting point of the customer product recommendation
         * is the customer product affinity Parquet file
         */
        val tableCPA = readCPA()
    
        val users = tableCPA.groupBy(x => x._2).map(_._1).collect
        val udict = ctx.sparkContext.broadcast(new RDict(users))
        
        val items = tableCPA.groupBy(x => x._3).map(_._1.toString).collect
        val idict = ctx.sparkContext.broadcast(new RDict(items))
        
        /*
         * STEP #2: Extract training parameters from request parameters or assign
         * default value, and build ratings from the customer product affinities
         */
        val partitions = req_params.get("partitions") match {
          case None => 20
          case Some(value) => value.toInt
        }
    
        val rank = req_params.get("rank") match { 
          case None => 10
          case Some(value) => value.toInt
          
        }
    
        val iter = req_params.get("iter") match { 
          case None => 20
          case Some(value) => value.toInt
        }

        val lambda = req_params.get("lambda") match { 
          case None => 0.01
          case Some(value) => value.toDouble
        }
        
        val trainset = tableCPA.map(x => {
          
          val ux = udict.value.getIndex(x._2)
          val ix = idict.value.getIndex(x._3.toString)
          
          Rating(ux,ix,x._4)
          
        })        
        /*
         * STEP #3: Train factorization matrix and evaluate the 
         * model on the training data, i.e. compute the RMSE
         */
        val model = ALS.train((trainset).repartition(partitions),rank,iter,lambda)        
        val rmse = computeRMSE(model,trainset)
        
        /*
         * STEP #4: Create MFModel and persist on the file system
         */
        val modelMF = new MFModel(rank,rmse,model.userFeatures,model.productFeatures)

        val store = String.format("""%s/%s/%s/2""",ctx.getBase,name,uid)         
        new MFUtil(ctx.sparkContext).write(store,udict.value.lookup,idict.value.lookup,modelMF)

        val end = new java.util.Date().getTime.toString
        ctx.putLog("info",String.format("""[UID: %s] %s learning finished at %s.""",uid,end))
 
        val res_params = Map(Names.REQ_MODEL -> name) ++ req_params
        context.parent ! LearnFinished(res_params)
      
        context.stop(self)
        
      } catch {
        case e:Exception => {
                    
          ctx.putLog("error",String.format("""[UID: %s] %s learning failed due to an internal error.""",uid,name))
          
          val res_params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params

          context.parent ! LearnFailed(res_params)            
          context.stop(self)
          
        }
    
      }
      
    }
    
  }
  
  private def computeRMSE(model:MatrixFactorizationModel,ratings:RDD[Rating]): Double = { 

    val dataset = ratings.map(x => (x.user,x.product))
    val predictions = model.predict(dataset).map(x => ((x.user,x.product),x.rating))
  
    Math.sqrt(ratings
      .map(x => ((x.user,x.product), x.rating)).join(predictions)
      .map{case ((ucol, icol), (r1, r2)) => {
             
        val err = (r1 - r2)
        err * err
            
      }}.mean())
    
  }

  private def readCPA():RDD[(String,String,Int,Double)] = {
    
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME).replace("CPR","CPA")
    
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)
    
    val parquetFile = sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(row => {

      val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]

      val item = data("item").asInstanceOf[Int]
      val value = data("value").asInstanceOf[Double]
    
      (site,user,item,value)
      
    })
    
  }

}