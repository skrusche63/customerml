package de.kp.insight.enrich
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.parquet._

import scala.collection.mutable.ArrayBuffer

private case class MarkovStep(step:Int,amount:Double,time:Long,state:String,score:Double)
/**
 * The CPFEnricher uses the machine learning results from the CPSLearner and
 * also the PRMLearner; note, that this implies that the respective Parquet
 * file names must be adjusted before the respective read request is performed.
 */
class CPFEnricher(ctx:RequestContext,params:Map[String,String]) extends BaseEnricher(ctx,params) {
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
        
  import sqlc.createSchemaRDD
  override def enrich {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val tableCPS = readCPS(params)
    val tableMSP = readMSP(params)
        
    /*
     * Build lookup table from MSP table
     */
    val lookup = ctx.sparkContext.broadcast(tableMSP.groupBy(x => x._1).map(x => {
          
      val last_state = x._1
      val data = x._2.map(v => (v._2,v._3,v._4)).toSeq.sortBy(v => v._1)
          
      (last_state,data)
          
    }).collect.toMap)

    val tableCPF = tableCPS.flatMap(x => {
      
      val forecasts = buildForecasts(x,lookup.value(x.state))
      forecasts.map(v => ParquetCPF(x.site,x.user,v.step,v.amount,v.time,v.state,v.score))
    
    })
         
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)                
    tableCPF.saveAsParquetFile(store)

  }  
  /*
   * The Intent Recognition engine returns a list of Markovian states; the ordering
   * of these states reflects the number of steps looked ahead
   */
  private def buildForecasts(record:ParquetCPS,states:Seq[(Int,String,Double)]):List[MarkovStep] = {
   
    val result = ArrayBuffer.empty[MarkovStep]
    val steps = states.size
    
    if (steps == 0) return result.toList
    
    /*
     * Retrieve quantile boundaries
     */
    val r_b1 = record.r_b1
    val r_b2 = record.r_b2
    val r_b3 = record.r_b3
    val r_b4 = record.r_b4
    val r_b5 = record.r_b5

    val s_b1 = record.s_b1
    val s_b2 = record.s_b2
    val s_b3 = record.s_b3
    val s_b4 = record.s_b4
    val s_b5 = record.s_b5
    
    (0 until steps).foreach(i => {
      
      val (step,state,score) = states(i)
        
      /* 
       * A MarkovState is a string that describes an amount
       * and timespan representation in terms of integers
       * from 1..5
       */
      val astate = state(0).toInt
      val sstate = state(1).toInt

      val ratio = (
        if (astate == 1) (r_b1 + 0) * 0.5
        else if (astate == 2) (r_b2 + r_b1) * 0.5
        else if (astate == 3) (r_b3 + r_b2) * 0.5
        else if (astate == 4) (r_b4 + r_b3) * 0.5
        else (r_b5 + r_b4) * 0.5
      )

      val span = (
        if (sstate == 1) (s_b5 + s_b4) * 0.5
        else if (sstate == 2) (s_b4 + s_b3) * 0.5
        else if (sstate == 3) (s_b3 + s_b2) * 0.5
        else if (sstate == 4) (s_b2 + s_b1) * 0.5
        else (s_b1 + 0) * 0.5
      )

      if (i == 0) {
        
        val next_amount = record.amount * ratio
        val next_time = Math.round(span) * DAY + record.timestamp
        
        val next_score = score
        
        result += MarkovStep(i+1,next_amount,next_time,state,next_score)
      
      } else {
        
        val previousStep = result(i-1)
        
        val next_amount = previousStep.amount * ratio
        val next_time   = Math.round(span) * DAY + previousStep.time
        
        val next_score = score * previousStep.score
        
        result += MarkovStep(i+1,next_amount,next_time,state,next_score)
        
      }
      
      
    })
    
    result.toList
    
  }

  /**
   * This method reads the Parquet file that specifies the state representation 
   * for all customers that have purchased at least twice since the start of the 
   * collection of the Shopify orders.
   */
  private def readCPS(params:Map[String,String]):RDD[ParquetCPS] = {
    
    val uid = params(Names.REQ_UID)
    
    val name = params(Names.REQ_NAME).replace("CPF","CPS")    
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)
    
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    val rawset = parquetFile.map(row => {

      val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]

      val amount = data("amount").asInstanceOf[Double]
      val timestamp = data("timestamp").asInstanceOf[Long]
      
      /*
       * Quantile boundaries for the amount ratio
       * and timespan with respect to subsequent
       * purchase transactions
       */
      val r_b1 = data("r_b1").asInstanceOf[Double]
      val r_b2 = data("r_b2").asInstanceOf[Double]
      val r_b3 = data("r_b3").asInstanceOf[Double]
      val r_b4 = data("r_b4").asInstanceOf[Double]
      val r_b5 = data("r_b5").asInstanceOf[Double]

      val s_b1 = data("s_b1").asInstanceOf[Double]
      val s_b2 = data("s_b2").asInstanceOf[Double]
      val s_b3 = data("s_b3").asInstanceOf[Double]
      val s_b4 = data("s_b4").asInstanceOf[Double]
      val s_b5 = data("s_b5").asInstanceOf[Double]

      val state = data("state").asInstanceOf[String]
            
      ParquetCPS(
          site,
          user,
          amount,
          timestamp,
          r_b1,
          r_b2,
          r_b3,
          r_b4,
          r_b5,
          s_b1,
          s_b2,
          s_b3,
          s_b4,
          s_b5,
          state
      )
      
    })
    
    rawset.groupBy(x => (x.site,x.user)).filter(_._2.size > 1).map(x => {
      x._2.toSeq.sortBy(_.timestamp).last      
    })

  }
  /**
   * This method reads a Parquet files that contains a Markovian state 
   * prediction for a predefined number of look ahead steps
   */
  private def readMSP(params:Map[String,String]):RDD[(String,Int,String,Double)] = {
    
    val uid = params(Names.REQ_UID)
    /**
     * The Markovian state predictions (MSP) are learned by the CPSLearner;
     * we therefore have to adjust the respective model name; this is done
     * by replacing just the base model name to respect the customer type
     * suffix
     */
    val name = params(Names.REQ_NAME).replace("CPF","CPS")    
    val store = String.format("""%s/%s/%s/2""",ctx.getBase,name,uid)
    
    /* 
     * Read in the parquet file created above. Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(record => {

      val values = record.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val last_state = data("last_state").asInstanceOf[String]      
      val step = data("step").asInstanceOf[Int]
      
      val state = data("state").asInstanceOf[String]
      val score = data("score").asInstanceOf[Double]
      
      (last_state,step,state,score)
    })
   
  }
  
}