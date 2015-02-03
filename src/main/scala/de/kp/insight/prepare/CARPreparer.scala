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

import de.kp.insight.preference.TFIDF
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

    val ds = orders.map(x => (x.site,x.user,x.ip_address,x.timestamp,x.group,x.amount,x.discount,x.shipping,x.items))
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
        
        case(site,user,ip_address,timestamp,group,amount,discount,shipping,items) => 
          ((site,user),(ip_address,timestamp,group,amount,discount,shipping,items))
      
      }.join(parquetCST).map{
        
        case((site,user),((ip_address,timestamp,group,amount,discount,shipping,items),rfm_type)) => 
          (site,user,ip_address,timestamp,group,amount,discount,shipping,items)
      }
      
    })     
    
    val table = filteredDS.groupBy(x => (x._1,x._2)).map(x => {    
      /*
       * Reduce to transaction data and sort with respect to timestamp
       */
      val trans = x._2.map(v => (v._3,v._4,v._5,v._6,v._7,v._8,v._9)).toSeq.sortBy(v => v._2)
      
      /* 
       * 1) (country, city) of the customer who made the purchase;
       *    this feature is introduced as a numerical feature
       */
      val locations = trans.map(_._1).map(ip => {
        
        val location = LocationFinder.locate(ip)
        
        val code = if (location.countrycode == "") "--" else location.countrycode
        val name = if (location.countryname == "") "N/A" else location.countryname
        
        CountryUtil.getIndex(code,name)
        
      })
      
    })

    // TODO
  }
}