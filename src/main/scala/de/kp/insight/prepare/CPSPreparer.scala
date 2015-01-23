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

import de.kp.spark.core.Names

import de.kp.insight.model._
import de.kp.insight.parquet._

import de.kp.insight.RequestContext

/**
 * The CPSPreparer generates a state representation for all customers
 * that have purchased at least twice since the start of the collection
 * of the Shopify orders.
 * 
 * Note, that we actually do not distinguish between customers that have 
 * a more frequent purchase behavior, and those, that have purchased only
 * twice.
 * 
 */
class CPSPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {
  
  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)

    val customer = params("customer").toInt
    /*
     * STEP #1: Restrict the purchase orders to those attributes that
     * are relevant for the state generation task; this encloses a
     * filtering with respect to customer type, if different from '0'
     */
    val ctype = sc.broadcast(customer)

    val ds = orders.map(x => (x.site,x.user,x.amount,x.timestamp))
    val filteredDS = (if (customer == 0) {
      /*
       * This customer type indicates that ALL customer types
       * have to be taken into account when computing the item
       * segmentation 
       */
      ds

    } else {
      /*
       * Load the Parquet file that specifies the customer type specification 
       * and filter those customers that match the provided customer type
       */
      val parquetCST = readCST(uid).filter(x => x._2 == ctype.value)      
      ds.map(x => ((x._1,x._2),(x._3,x._4))).join(parquetCST).map(x => {

    	val ((site,user),((amount,timestamp),rfm_type)) = x
    	(site,user,amount,timestamp)

  	  })
    })     

    /*
     * STEP #1: We calculate the amount ratios and timespans from
     * subsequent customer specific purchase transactions and then
     * do a quantile analysis to find univariate boundaries 
     */
    val rawset = filteredDS.groupBy(x => (x._1,x._2)).filter(_._2.size > 1).map(x => {

      /* Compute time ordered list of (amount,timestamp) */
      val data = x._2.map(v => (v._3,v._4)).toList.sortBy(_._2)      

      val amounts = data.map(_._1)       
      val ratios = amounts.zip(amounts.tail).map(v => v._2 / v._1)

      val timestamps = data.map(_._2)
      val timespans = timestamps.zip(timestamps.tail).map(v => v._2 - v._1).map(v => (if (v / DAY < 1) 1 else v / DAY))

      (ratios,timespans)

    })
       
    val ratios = rawset.flatMap(_._1).sortBy(x => x)
    val r_quintiles = sc.broadcast(quintiles(ratios))

    val spans = rawset.flatMap(_._2).map(_.toDouble).sortBy(x => x)
    val s_quintiles = sc.broadcast(quintiles(spans))

    val table = filteredDS.groupBy(x => (x._1,x._2)).filter(_._2.size > 1).flatMap(x => {

      val (site,user) = x._1  

      /* Compute time ordered list of (amount,timestamp) */
      val data = x._2.map(v => (v._3,v._4)).toList.sortBy(_._2)      

      val amounts = data.map(_._1)    
      val amount_ratios = amounts.zip(amounts.tail).map(v => v._2 / v._1)

      /*
       * We introduce a rating from 1..5 for the amount ratio attribute
       * and assign 5 to the highest value, 4 to a less valuable etc
       */     
      val r_b1 = r_quintiles.value(0.20)
      val r_b2 = r_quintiles.value(0.40)
      val r_b3 = r_quintiles.value(0.60)
      val r_b4 = r_quintiles.value(0.80)
      val r_b5 = r_quintiles.value(1.00)
          
      val amount_states = amount_ratios.map(ratio => {
            
        val rval = (
          if (ratio < r_b1) 1
          else if (r_b1 <= ratio && ratio < r_b2) 2
          else if (r_b2 <= ratio && ratio < r_b3) 3
          else if (r_b3 <= ratio && ratio < r_b4) 4
          else if (r_b4 <= ratio) 5
          else 0  
      )

      if (rval == 0) throw new Exception("rval = 0 is not support.")
      rval.toString
            
    })

    val timestamps = data.map(_._2)
    val timespans = timestamps.zip(timestamps.tail).map(v => (if ((v._2 - v._1) / DAY < 1) 1.toInt else ((v._2 - v._1) / DAY)).toInt)

    /*
     * We introduce a rating from 1..5 for the timespan attribute
     * and assign 5 to the lowest value, 4 to a higher valuable etc
     */     
    val s_b1 = s_quintiles.value(0.20)
    val s_b2 = s_quintiles.value(0.40)
    val s_b3 = s_quintiles.value(0.60)
    val s_b4 = s_quintiles.value(0.80)
    val s_b5 = s_quintiles.value(1.00)

    val timespan_states = timespans.map(span => {
            
      val sval = (
        if (span < s_b1) 5
        else if (s_b1 <= span && span < s_b2) 4
        else if (s_b2 <= span && span < s_b3) 3
        else if (s_b3 <= span && span < s_b4) 2
        else if (s_b4 <= span) 1
        else 0  
      )

      if (sval == 0) throw new Exception("sval = 0 is not support.")
      sval.toString
            
    })

    val states = amount_states.zip(timespan_states).map(x => x._1 + x._2)
    /*
     * Note, that amount ratios, timespans, amount & timestamp tail
     * and states are ordered appropriately
     */
    val ds = amounts.tail.zip(timestamps.tail).zip(states)         
    ds.map(v => {
          
      val ((amount, timestamp), state) = v
      ParquetCPS(
    		  site,
    		  user,
    		  amount,
    		  timestamp,
    		  /*
    		   * The quantile boundaries are provided here
    		   * in a de-normalized manner to enable quick
    		   * forecast modeling after having determined
    		   * the state predictions
    		   */
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
    })
    /* 
     * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
     * allowing it to be stored using Parquet. 
     */
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    table.saveAsParquetFile(store)
  
  }
  
}