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
 * The PPFPreparer is responsible for a quantile-based segmentation of
 * purchased items with respect to the customer and purchase frequency.
 * 
 * The respective frequencies are specified by rating numbers from 1..5,
 * where 5 means the highest valuable value from a business perspective.
 * 
 * The items that refer to a certain purchase period of time and a specific
 * customer type (with respect to the prior RFM segmentation) are mapped
 * onto a two dimensional space and divided into 25 different segments 
 * due to the selected quantile mechanism, from 11 to 55.
 * 
 * Items in the 55 segment have the highest customer and also purchase
 * frequency, while 11 describes items with the lowest customer and also
 * purchase frequency.
 */
class PPFPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {
  
  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
      
    val customer = params("customer").toInt
    /*
     * STEP #1: Restrict the purchase orders to those items and attributes
     * that are relevant for the item segmentation task; this encloses a
     * filtering with respect to customer type, if different from '0'
     */
    val ctype = sc.broadcast(customer)

    val ds = orders.flatMap(x => x.items.map(v => (x.site,x.user,v.item,v.quantity)))
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

    	val ((site,user),((item,quantity),rfm_type)) = x
    	(site,user,item,quantity)

      })
    })       
    /*
     * STEP #2: Build item customer frequency distribution; note, that
     * we are not interested here, how often a certain customer has
     * bought a specific product.
     * 
     * The data base used here describes the purchases customers have 
     * made within a certain period of time. This implies, that we are 
     * NOT faced with zero values, i.e. items with zero purchase frequency
     */
    val ds1 = filteredDS.groupBy(_._3).map(x => {

      val item = x._1
      val csup = x._2.map(v => (v._1,v._2)).toSeq.distinct.size

      (item,csup)

    })
    val c_quintiles = sc.broadcast(quintiles(ds1.map(_._2.toDouble).sortBy(x => x)))        
    /*
     * STEP #3: Build item purchase frequency distribution; here we are
     * not interested into the specific user, but the purchase quantity
     */
    val ds2 = filteredDS.groupBy(_._3).map(x => (x._1, x._2.map(_._4).sum))
    val p_quintiles = sc.broadcast(quintiles(ds2.map(_._2.toDouble).sortBy(x => x)))
    /*
     * STEP #4: Build customer purchase space from customer frquency 
     * and purchase frequency; frequency values with the highest value
     * for the business company are assigned a 5, less valuable 4 and
     * so forth.
     */
    val ds3 = ds1.map(x => {
          
      val (item,freq) = x
          
      val b1 = c_quintiles.value(0.20)
      val b2 = c_quintiles.value(0.40)
      val b3 = c_quintiles.value(0.60)
      val b4 = c_quintiles.value(0.80)
      
      val cval = (
        if (freq < b1) 1
        else if (b1 <= freq && freq < b2) 2
        else if (b2 <= freq && freq < b3) 3
        else if (b3 <= freq && freq < b4) 4
        else if (b4 <= freq) 5
        else 0  
      )

      (item,(freq,cval))
          
    })
        
    val ds4 = ds2.map(x => {
          
      val (item,freq) = x
          
      val b1 = p_quintiles.value(0.20)
      val b2 = p_quintiles.value(0.40)
      val b3 = p_quintiles.value(0.60)
      val b4 = p_quintiles.value(0.80)

      val pval = (
        if (freq < b1) 1
        else if (b1 <= freq && freq < b2) 2
        else if (b2 <= freq && freq < b3) 3
        else if (b3 <= freq && freq < b4) 4
        else if (b4 <= freq) 5
        else 0  
      )

      (item,(freq,pval))
          
    })
        
    val table = ds3.join(ds4).map(x => {

      val item = x._1
      val ((cfreq,cval),(pfreq,pval)) = x._2

      ParquetPPF(item,cfreq,pfreq,cval,pval)

    })

    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    table.saveAsParquetFile(store)
  
  }
  
}