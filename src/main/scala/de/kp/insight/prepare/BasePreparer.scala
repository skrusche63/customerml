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

import com.twitter.algebird._
import com.twitter.algebird.Operators._

import de.kp.spark.core.Names

import de.kp.insight._
import de.kp.insight.model._

abstract class BasePreparer(ctx:RequestContext) extends BaseActor(ctx) {
  /*
   * The parameter K is used as an initialization 
   * prameter for the QTree semigroup
   */
  private val K = 6
  private val QUINTILES = List(0.20,0.40,0.60,0.80,1.00)
        
  protected val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data
      
      val uid = req_params(Names.REQ_UID)
      val name = req_params(Names.REQ_NAME)
      
      try {
      
        val start = new java.util.Date().getTime.toString            
        ctx.putLog("info",String.format("""[UID: %s] %s preparation request received at %s.""",uid,name,start))
        
        prepare(req_params)

        val end = new java.util.Date().getTime
        ctx.putLog("info",String.format("""[UID: %s] %s preparation finished at %s.""",uid,name,end.toString))

        val params = Map(Names.REQ_MODEL -> name) ++ req_params
        context.parent ! PrepareFinished(params)

      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          ctx.putLog("error",String.format("""[UID: %s] %s preparation exception: %s.""",uid,name,e.getMessage))
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }
  
  protected def prepare(params:Map[String,String])
  
  /**
   * This method calculates the quantile boundaries with
   * respect to a certain quantile from a set of Doubles
   */
  protected def boundary(dataset:Seq[Double],quantile:Double):Double = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val qtree = dataset.map(v => QTree(v)).reduce(_ + _) 
    
    val (lower,upper) = qtree.quantileBounds(quantile)
    val mean = (lower + upper) / 2
    
    mean
    
  }
  
  /**
   * This method calculates the quantile boundaries from an
   * array of Double by using the QUINTILE specification
   */
  protected def quintiles(dataset:RDD[Double]):Map[Double,Double] = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val qtree = dataset.map(v => QTree(v)).reduce(_ + _) 
    QUINTILES.map(x => {
      
      val (lower,upper) = qtree.quantileBounds(x)
      val mean = (lower + upper) / 2

      (x,mean)
      
    }).toMap
    
  }
  
  /**
   * This method loads the customer type description from the
   * Parquet file that has been created by the RFMPreparer.
   * 
   * The customer type specification is used to filter other
   * customer datasets by an RDD join mechanism to reduce to
   * those records that refer to a certain customer type
   */
  protected def readCST(uid:String):RDD[((String,String),Int)] = {

    val store = String.format("""%s/CST/%s""",ctx.getBase,uid)         
    
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
      
      val rfm_type = data("rfm_type").asInstanceOf[Int]
      ((site,user),rfm_type)      
    
    })
    
  }

}