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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.joda.time.DateTime

import com.twitter.algebird._
import com.twitter.algebird.Operators._

import de.kp.spark.core.Names

import de.kp.insight.model._
import de.kp.insight.parquet._

import de.kp.insight.RequestContext

/**
 * The RFM Customer Segmentation model is an embarrassingly simple way of 
 * segmenting the customer base inside a marketing database. The resulting 
 * groups are easy to understand, analyze and action without the need of 
 * going through complex mathematics.
 * 
 */
class RFMPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {
        
  /*
   * The parameter K is used as an initialization 
   * parameter for the QTree semigroup
   */
  private val K = 6
  private val QUINTILES = List(0.20,0.40,0.60,0.80,1.00)
  
  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)

    /*
     * The first step in RFM Customer Segmentation is to define the three attributes. 
     * The model allows for a certain flexibility with definitions and you can adjust 
     * them to the specifics of your business. 
     * 
     * The three attributes are:
     * 
     * - Recency which represents the “freshness” of customer activity. Naturally, we 
     *   would like to identify active and inactive customers. The basic definition for 
     *   this attribute is the number of days since last order or interaction.
     * 
     * - Frequency captures how often the customer buys. By default this could be the 
     *   total number of orders in the last year or during the whole of customer’s lifetime.
     * 
     * - Monetary value indicates how much the customer is spending. In many cases this 
     *   is just the sum of all order values.
     *   
     */
    val rawset = orders.groupBy(x => (x.site,x.user)).map(p => {

      val (site,user) = p._1  
      
      /* Compute time ordered list of (amount,timestamp) */
      val s0 = p._2.map(x => (x.amount,x.timestamp)).toList.sortBy(_._2)      

      val today = new DateTime().getMillis
      val recency = ((s0.map(_._2).last - today) / DAY).toInt
      /*
       * Frequency is the total number of orders made by
       * a certain customer during the whole lifetime
       */
      val frequency = s0.size 
      /* 
       * Monetary attribute is calculated as the sum
       * of all order values 
       */
      val monetary = s0.map(_._1).sum
          
      (site,user,today,recency,frequency,monetary)

    })
        
    /*
     * Now that the RFM rawset is ready, we allocate the respective attribute values
     * into segments. To this end, we use quantiles. Simply put, this means folllowing:
     * 
     * The k-th quantile is a value x such that k% of values are less than x. So if the 
     * 25th quantile is 10, then 25% of all values are smaller than 10. 
     * 
     * As we try to compute valulabe data as early as possible, the RFM table is used
     * to calulcate the quantiles for the different attributes and the result are
     * re-assigned.
     */
    val r_quantiles = sc.broadcast(RQuintiles(rawset))
    val f_quantiles = sc.broadcast(FQuintiles(rawset))

    val m_quantiles = sc.broadcast(MQuintiles(rawset))
        
    val dataset = rawset.map(x => {
          
      val (site,user,today,recency,frequency,monetary) = x
          
      /*
       * We introduce a rating from 1..5 for the recency, frequency and monetary
       * attribute, and assign 5 to most valuable value, 4 to a less valuable etc;
       * note, that we use numeric values for the respective ratings as we compute
       * statistics from them and also do clustering with respect to the RFM values
       */

      /********** RECENCY ***********/

      val r_b1 = r_quantiles.value(0.20)
      val r_b2 = r_quantiles.value(0.40)
      val r_b3 = r_quantiles.value(0.60)
      val r_b4 = r_quantiles.value(0.80)

      val rval = (
        if (recency < r_b1) 5
        else if (r_b1 <= recency && recency < r_b2) 4
        else if (r_b2 <= recency && recency < r_b3) 3
        else if (r_b3 <= recency && recency < r_b4) 2
        else if (r_b4 <= recency) 1
        else 0  
      )
                    
      /********** FREQUENCY *********/

      val f_b1 = r_quantiles.value(0.20)
      val f_b2 = r_quantiles.value(0.40)
      val f_b3 = r_quantiles.value(0.60)
      val f_b4 = r_quantiles.value(0.80)

      val fval = (
        if (frequency < f_b1) 1
        else if (f_b1 <= frequency && frequency < f_b2) 2
        else if (f_b2 <= frequency && frequency < f_b3) 3
        else if (f_b3 <= frequency && frequency < f_b4) 4
        else if (f_b4 <= frequency) 5
        else 0  
      )

      /********** MONETARY **********/

      val m_b1 = r_quantiles.value(0.20)
      val m_b2 = r_quantiles.value(0.40)
      val m_b3 = r_quantiles.value(0.60)
      val m_b4 = r_quantiles.value(0.80)

      val mval = (
        if (monetary < m_b1) 1
        else if (m_b1 <= monetary && monetary < m_b2) 2
        else if (m_b2 <= monetary && monetary < m_b3) 3
        else if (m_b3 <= monetary && monetary < m_b4) 4
        else if (m_b4 <= monetary) 5
        else 0  
      )

      val RFM = String.format("""%s%s%s""",rval.toString,fval.toString,mval.toString).toInt

      (site,user,today,recency,rval,frequency,fval,monetary,mval)
        
    })
    /*
     * From the respective rating values, we derive statistical parameters 
     * and finally use these parameters to divide the customer RFM space 
     * into 8 different customer types 
     */
    val r_stats = sc.broadcast(dataset.map(_._5).stats())
    val f_stats = sc.broadcast(dataset.map(_._7).stats())
    val m_stats = sc.broadcast(dataset.map(_._9).stats())

    val tableRFM = dataset.map(x => {
          
      val (site,user,today,recency,rval,frequency,fval,monetary,mval) = x
          
      val r_mean = r_stats.value.mean
      val r_state = if (rval > r_mean) "H" else "L"

      val f_mean = f_stats.value.mean
      val f_state = if (fval > f_mean) "H" else "L"

  	  val m_mean = m_stats.value.mean
  	  val m_state = if (mval > m_mean) "H" else "L"
  	  /*
  	   * From the evaluation of the R,F,M values with respect to
  	   * the 
  	   */
  	  val rfm_state = r_state + f_state + m_state          
  	  val rfm_type = rfm_state match {
            
        case "HHH" =>           
          /*
           * MOST VALUABLE
           * 
           * This customer is specified as category "1"
           * that indicates the most valuable customer
           */
          1
        case "HLH" =>
          /*
           * VALUABLE
           * 
           * This customer is specified as category "2"
           * that indicates valuable customers
           */
          2
        case "HHL" =>
          /*
           * SHOPPER
           * 
           * This customer is specified as category "3"
           * that indicates shoppers
           */
          3
        case "HLL" => 
          /*
           * FIRST-TIMER
           * 
           * This customer is specified as category "4"
           * that indicate customers that have recently
           * joined the company
           */
          4
        case "LHH" => 
          /*
           * CHURNER
           * 
           * This customer is specified as category "5"
           * that indicates potential churners; note, that
           * is kind of churn detection is not really used
           * as an alternative method is supported here
           */
          5
        case "LHL" => 
          /*
           * FREQUENT
           * 
           * This customer is specified as category "6"
           * that indicates frequent buyers
           */
          6
        case "LLH" => 
          /*
           * SPENDER
           * 
           * This customer is specified as category "7"
           * that indicates spenders
           */
          7
        case "LLL" => 
          /*
           * UNCERTAIN
           * 
           * This customer is specified as category "8"
           * that indicates uncertain customers or those
           * with a high cost than profit rate
           */
          8

        case _ => throw new Exception("Unknown RFM state detected.")
          
      }
        
      ParquetRFM(site,user,today,recency,frequency,monetary,rval,fval,mval,rfm_type)
          
    })
    /* 
     * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
     * allowing it to be stored as Parquet file. Note, that the Parquet
     * file specifies a certain RFM model that takes all registered orders
     * up to now into account.
     */
    val store_rfm = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    tableRFM.saveAsParquetFile(store_rfm)

    /*
     * We need to store the ParquetCST table as well; this table assigns
     * a certain customer (site,user) to a specific customer type
     */
    val tableCST = tableRFM.map(x => {ParquetCST(x.site,x.user,x.rfm_type)})

    val store_cst = String.format("""%s/CST/%s""",ctx.getBase,uid)         
    tableCST.saveAsParquetFile(store_cst)
  
  }
  
  private def RQuintiles(dataset:RDD[(String,String,Long,Int,Int,Double)]):Map[Double,Double] = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val d0 = dataset.map(_._4.toDouble).collect.toSeq.sorted
    val d1 = d0.map(v => QTree(v)).reduce(_ + _) 

    QUINTILES.map(x => {
      
      val (lower,upper) = d1.quantileBounds(x)
      val mean = (lower + upper) / 2

      (x,mean)
      
    }).toMap
    
  }
  
  private def FQuintiles(dataset:RDD[(String,String,Long,Int,Int,Double)]):Map[Double,Double] = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val d0 = dataset.map(_._5.toDouble).collect.toSeq.sorted
    val d1 = d0.map(v => QTree(v)).reduce(_ + _) 

    QUINTILES.map(x => {
      
      val (lower,upper) = d1.quantileBounds(x)
      val mean = (lower + upper) / 2

      (x,mean)
      
    }).toMap
    
  }
  
  private def MQuintiles(dataset:RDD[(String,String,Long,Int,Int,Double)]):Map[Double,Double] = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val d0 = dataset.map(_._6).collect.toSeq.sorted
    val d1 = d0.map(v => QTree(v)).reduce(_ + _) 

    QUINTILES.map(x => {
      
      val (lower,upper) = d1.quantileBounds(x)
      val mean = (lower + upper) / 2

      (x,mean)
      
    }).toMap
    
  }
  
}