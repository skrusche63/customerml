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

import de.kp.insight.preference.TFIDF
import de.kp.insight.RequestContext

/**
 * The CSAPreparer generates the timespan affinity in terms
 * of days from all orders registered so far, and for every 
 * customer that has purchased at least twice
 */
class CSAPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {
 
  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)

    val customer = params("customer").toInt
    /*
     * STEP #1: Calculate the timespan in days between two susequent purchase
     * transactions and the respective frequency; from the result derive a 
     * quintile based segmentation that is used as a categorization of the 
     * customer-specific timespans 
     */
    val ds1 = orders.map(x => (x.site,x.user,x.timestamp)).groupBy(x => (x._1,x._2)).filter(_._2.size > 1)
    val ds2 = ds1.flatMap(x => {
          
      val (site,user) = x._1
      /*
       * Calculate the timespans between two subsequent transactions
       * and represent the result into terms of days; doing this, we
       * also describe the timespans less than a day as a day
       */ 
      val timestamps = x._2.map(_._3).toSeq.sorted
      val timespans = timestamps.zip(timestamps.tail).map(v => v._2 - v._1).map(v => (if (v / DAY < 1) 1 else v / DAY).toInt)         
      /*
       * Calculate the frequency of the different timespans; this support 
       * is used to distinguish between high frequent, normal and low 
       * frequent buyers.
       */
      val supp = timespans.groupBy(v => v).map(v => (v._1,v._2.size))
      supp.map(v => {

    	val (span,freq) = v
    	(site,user,span,freq)

      })
          
    })
        
    val ds3 = ds2.groupBy(x => x._3).map(x => (x._1,x._2.map(_._4).sum))
    val s_quintiles = sc.broadcast(quintiles(ds3.map(_._2.toDouble).sortBy(x => x)))
    /*
     * The quintiles are used to assign a category or label to the 
     * different timespans; the smallest timespan is assigned 5,
     * the next 4 and so forth
     */
    val ds4 = ds2.map(x => {
          
      val (site,user,span,freq) = x
          
      val b1 = s_quintiles.value(0.20)
      val b2 = s_quintiles.value(0.40)
      val b3 = s_quintiles.value(0.60)
      val b4 = s_quintiles.value(0.80)
      
      val sval = (
        if (freq < b1) 5
        else if (b1 <= freq && freq < b2) 4
        else if (b2 <= freq && freq < b3) 3
        else if (b3 <= freq && freq < b4) 2
        else if (b4 <= freq) 1
        else 0  
      ).toString

      (site,user,span,freq,sval)
          
    })
    /*
     * STEP #2: Restrict the timespan distribution to those entries of
     * a certain customer type; note, that a customer type of value '0'
     * specifies that ALL customers have to be taken into account
     */
    val ctype = sc.broadcast(customer)

    val filteredDS = (if (customer == 0) {
      /*
       * This customer type indicates that ALL customer types
       * have to be taken into account
       */
      ds4

    } else {
      /*
       * Load the Parquet file that specifies the customer type specification 
       * and filter those customers that match the provided customer type
       */
      val parquetCST = readCST(uid).filter(x => x._2 == ctype.value)      
      ds4.map(x => ((x._1,x._2),(x._3,x._4,x._5))).join(parquetCST).map(x => {

    	val ((site,user),((span,freq,sval),rfm_type)) = x
    	(site,user,span,freq,sval)

      })
    })       
    val table = TFIDF.computeCSA(filteredDS)
    /* 
     * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
     * allowing it to be stored using Parquet. 
     */
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    table.saveAsParquetFile(store)
  
  }

}