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

import de.kp.spark.core.Names

import de.kp.insight.model._
import de.kp.insight.parquet._

import de.kp.insight.RequestContext

/**
 * POMPreparer is responsible for generating the purchase overview
 * metrix (POM) from the purchase orders of a certain period of time
 */
class POMPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {

  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)

    val total = orders.count

    /********************* MONETARY DIMENSION ********************/

    /*
     * Compute the average, minimum and maximum amount of all 
     * the purchases in the purchase history provided by orders
     */    
    val amounts = orders.map(_.amount)
    val m_stats = amounts.stats

    val m_mean  = m_stats.mean

    val m_min = m_stats.min
    val m_max = m_stats.max

    val m_stdev = m_stats.stdev
    val m_sum   = m_stats.sum

    val m_variance = m_stats.variance
    
    /********************* TEMPORAL DIMENSION ********************/

    /*
     * Compute the average,minimum and maximum time elapsed between 
     * two subsequent transactions; the 'zip' method is used to pairs 
     * between two subsequent timestamps
     */
    val timestamps = orders.map(x => (x.site,x.user,x.timestamp))
    val timespans = timestamps.groupBy(x => (x._1,x._2)).filter(_._2.size > 1).flatMap(x => {
          
      val (site,user) = x._1
      val time = x._2.map(_._3).toSeq.sorted
          
      time.zip(time.tail).map(x => x._2 - x._1).map(v => (if (v / DAY < 1) 1 else v / DAY).toInt)
          
    })
    
    val t_stats = timespans.stats
    val t_mean  = t_stats.mean

    val t_min = t_stats.min
    val t_max = t_stats.max

    val t_stdev = t_stats.stdev
    val t_variance = t_stats.variance

    /* Day of the week: 1..7 */
    val day_freq = timestamps.map(x => new DateTime(x).dayOfWeek().get).groupBy(x => x).map(x => (x._1,x._2.size)).collect.toSeq

    /* Time of day: 0..23 */
    val hour_freq = timestamps.map(x => new DateTime(x).hourOfDay().get).groupBy(x => x).map(x => (x._1,x._2.size)).collect.toSeq

    /*********************** ITEM DIMENSION **********************/

    val items = orders.flatMap(x => x.items.map(v => (v.item,v.quantity)))
    val item_freq = items.groupBy(x => x._1).map(x => (x._1,x._2.map(_._2).sum)).collect.toSeq

    val item_total = items.map(_._1).distinct.count

    /*********************** CUSTOMER DIMENSION ******************/
    
    val customer_total = orders.groupBy(x => (x.site,x.user)).count
      
    /*
     * Build Parquet file 
     */
    val table = ctx.sparkContext.parallelize(List(
    	ParquetPOM(  
            total,
  
            /********** AMOUNT DIMENSION **********/
  
            m_sum,
            m_mean,
            m_max,
            m_min,
            m_stdev,
            m_variance,
  
            /********** TEMPORARL DIMENSION **********/
 
            t_mean,
            t_max,
            t_min,
            t_stdev,
            t_variance,

            day_freq,
            hour_freq,
  
            /********** PRODUCT DIMENSION **********/

            item_freq,
            item_total,
  
            /********** CUSTOMER DIMENSION **********/
    
            customer_total
            
      )))
        
    /* 
     * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
     * allowing it to be stored using Parquet. 
     */
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    table.saveAsParquetFile(store)

  }
}