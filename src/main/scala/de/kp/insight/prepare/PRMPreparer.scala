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
 * The PRMPreparer prepares the purchase transactions of a certain
 * period of time and a specific customer type for association rule
 * mining with Predictiveworks' Association Analysis engine
 */
class PRMPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {

  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)

    val customer = params("customer").toInt
    /*
     * Association rule mining determines items that are frequently
     * bought together; to this end, we filter those transactions
     * with more than one item purchased 
     */
    val ds = orders.filter(x => x.items.size > 1).flatMap(x => x.items.map(v => (x.site,x.user,x.timestamp,x.group,v.item)))
    /*
     * STEP #1: Restrict the purchase orders to those records that match
     * the provided customer type. In case of customer type = '0', the 
     * 'user' provided with the ParquetASR table is the customer itself.
     * 
     * In all other cases, the 'user' attribute is replaced by the customer
     * type and the respective transactions are assigned to this type
     */
    val ctype = sc.broadcast(customer)
    val table = (if (customer == 0) {
      /*
       * This customer type indicates that ALL customer types
       * have to be taken into account when computing the item
       * segmentation 
       */
      ds.map(x => ParquetASR(x._1,x._2,x._3,x._4,x._5))

    } else {
      /*
       * Load the Parquet file that specifies the customer type specification 
       * and filter those customers that match the provided customer type
       */
      val parquetCST = readCST(uid).filter(x => x._2 == ctype.value)      
      ds.map(x => ((x._1,x._2),(x._3,x._4,x._5))).join(parquetCST).map{

        case ((site,user),((timestamp,group,item),rfm_type)) =>
    	/*
    	 * Note, that we replace the 'user' by the respective rfm_type
    	 */
    	ParquetASR(site,rfm_type.toString,timestamp,group,item)

      }
    })               
    /* 
     * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
     * allowing it to be stored using Parquet. 
     */
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    table.saveAsParquetFile(store)
  
  }
  
}