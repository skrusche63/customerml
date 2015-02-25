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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.joda.time.DateTime

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.model._

class LOCEnricher(ctx:RequestContext,params:Map[String,String]) extends BaseEnricher(ctx,params) {
  
  private val WORKING_DAY_MIN = 1
  private val WORKING_DAY_MAX = 5
  
  private val WORKING_HOUR_MIN = 8
  private val WORKING_HOUR_MAX = 17

  private val AVERAGE_RADIUS_OF_EARTH = 6371.toDouble
        
  import sqlc.createSchemaRDD
  override def enrich {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
      
    val site = params(Names.REQ_SITE)
    
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    val tableLOC = readLOC(store)
    
    val tableGEO = tableLOC.groupBy(x => (x._1,x._2)).map(x => {
      
      val (site,user) = x._1
      val data = x._2.toSeq.sortBy(_._4)
      
      val ds1 = data.map(x => (x._4,x._14,x._15))
      
//      val timestamp = x.timestamp
//      /*
//       * Determine the day of the week, and also the time of the day
//       * and infer from this, whether the respective purchase has been
//       * performed during working hour or non-working hour
//       */
//      val datetime = new DateTime(timestamp)
//        
//      val day = datetime.dayOfWeek().get
//      val hour = datetime.hourOfDay().get
//        
//      val working_hour = if (
//          WORKING_DAY_MIN <= day && day <= WORKING_DAY_MAX && 
//          WORKING_HOUR_MIN <= hour && hour <= WORKING_HOUR_MAX
//        
//      ) true else false
  
    })
    
  }
  
  /*
   * Geospatial distance based on Haversine formula
   */
  private def calculateDistance(lat1:Double,lon1:Double,lat2:Double,lon2:Double):Int = {

    val latd2 = (Math.toRadians(lat1 - lat2) / 2)
    val lond2 = (Math.toRadians(lon1 - lon2) / 2)

    val a = Math.sin(latd2) * Math.sin(latd2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lond2) * Math.sin(lond2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val d = AVERAGE_RADIUS_OF_EARTH * c
    
    return (Math.round(d.toFloat))
  }

  private def readLOC(store:String):RDD[(String,String,String,Long,String,String,String,String,Int,Int,Int,String,String,Double,Double)] = {
    
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlc.parquetFile(store)
    val schema = ctx.sparkContext.broadcast(parquetFile.schema)
    
    parquetFile.map(row => {

      val data = schema.value.fields.zip(row.toSeq).map{case(field,value) => (field.name,value)}.toMap

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]
      
      val ip_address = data("ip_address").asInstanceOf[String]
      val timestamp = data("timestamp").asInstanceOf[Long]

      val countryname = data("countryname").asInstanceOf[String]
      val countrycode = data("countrycode").asInstanceOf[String]

      val region = data("region").asInstanceOf[String]
      val regionname = data("regionname").asInstanceOf[String]

      val areacode = data("areacode").asInstanceOf[Int]
      val dmacode = data("dmacode").asInstanceOf[Int]

      val metrocode = data("metrocode").asInstanceOf[Int]
      val city = data("city").asInstanceOf[String]
 
      val postalcode = data("postalcode").asInstanceOf[String]

      val lat = data("lat").asInstanceOf[Double]
      val lon = data("lon").asInstanceOf[Double]

      (
          site,
          user,
  
          ip_address,
          timestamp,
    
          countryname,
          countrycode,

          region,
          regionname,
  
          areacode,
          dmacode,
  
          metrocode,
          city,
  
          postalcode,
	  
          lat,
          lon
      )
      
    })

  }

}