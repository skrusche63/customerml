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

import de.kp.insight.geoip.{CountryUtil,LocationFinder}
/**
 * CARPreparer has a focus on customers that refer to a certain 
 * customer type (from RFM Analysis); this approach ensures that
 * factorization models and similaity matrix are trained for a 
 * certain customer segment to avoid a mixing of e.g. bust buyers
 * with those that tend to fade off.
 */
class CARPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {
  
  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
      
    val customer = params("customer").toInt
    /*
     * STEP #1: Restrict the purchase orders to those items and attributes
     * that are relevant for the CAR preparation task; this encloses a
     * filtering with respect to customer type, if different from '0'
     */
    val ctype = sc.broadcast(customer)

    val ds = orders.map(x => (x.site,x.user,x.ip_address,x.timestamp,x.items))
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
      ds.map{
        
        case(site,user,ip_address,timestamp,items) => 
          ((site,user),(ip_address,timestamp,items))
      
      }.join(parquetCST).map{
        
        case((site,user),((ip_address,timestamp,items),rfm_type)) => 
          (site,user,ip_address,timestamp,items)
      }
      
    })     
    /*
     * STEP #2: Build the features associated with the Context-Aware recommendation
     * approach from every customer's transaction data 
     */
    val table = filteredDS.groupBy(x => (x._1,x._2)).flatMap(p => {    

      val (site,user) = p._1
      val trans = p._2.map{case(site,user,ip_address,timestamp,items) => (ip_address,timestamp,items)}
      
      val geolookup = trans.map{case(ip_address,timestamp,items) => (ip_address,timestamp)}.map{case(ip_address,timestamp) => {
        
        val location = LocationFinder.locate(ip_address)
        
        val code = if (location.countrycode == "") "--" else location.countrycode
        val name = if (location.countryname == "") "N/A" else location.countryname
        
        (timestamp,CountryUtil.getIndex(code,name))
        
      }}.toMap
      
      /*
       * Compute time ordered list of purchased items and associated items 
       * (in the same transaction); for each item, we are interested in all
       * the items that have been purchased together 
       */
      val items = trans.flatMap{
        case(ip_address,timestamp,items) => {
          
          val iids = items.map(_.item)
          iids.map{case (iid) => (iid,timestamp,iids)}}
      
      }.groupBy{case(iid,timestamp,iids) => iid}
      /*
       * In order to gather contextual information for a certain (active) item,
       * we compare the two latest customer transactions, where the respective
       * item was bought
       */
      items.map(x => {
        
        /*
         * The active item is a categorical feature; this implies
         * that the respective CARLearner must set the value to 1
         */
        val active_item = x._1
        /*
         * Determine distint set of items that have been purchased
         * together with the active item; these data are used as a 
         * categorical set, and therefore the value must be set to 
         * 1 / N by the CARLearner
         */
        val other_items = x._2.flatMap(_._3).toSeq.distinct
        /*
         * Determine the recency of the last purchase of the active
         * item. The time elapsed from the last transaction since
         * now (today) is an indicator of the attractivity of the
         * active item. 
         * 
         * The recency is used as a numerical context variable
         */
        val timestamps = x._2.map(_._2).toSeq.sorted
       
        val latest = timestamps.last
        val today = new DateTime().getMillis()
        
        val recency = ((today - latest) / DAY).toDouble
        /*
         * From the timestamps, we are able to determine the 
         * geo location of the respective transaction; for 
         * the context, we determine the location with the
         * highest frequency with respect to customers country
         */       
        val location = timestamps.map(geolookup(_))
                         // Compute location count
                         .groupBy(v => v).map(v => (v._1,v._2.size))
                         // Determine location with highest cont
                         .toSeq.sortBy(_._2).last._1
        
        ParquetCAR(site,user,today,active_item,other_items,recency,location)
        
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