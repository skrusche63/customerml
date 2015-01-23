package de.kp.insight.model
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

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}
import de.kp.spark.core.model._
import de.kp.insight.shopify.ShopifyProduct

case class ActorInfo(
  name:String,timestamp:Long
)

case class ActorStatus(
  name:String,date:String,status:String
)

case class ActorsStatus(items:List[ActorStatus])

case class StopActor()

/****************************************************************************
 * 
 *                      SUB PROCESS 'ANALYZE'
 * 
 ***************************************************************************/

case class StartAnalyze(data:Map[String,String])

case class AnalyzeFailed(data:Map[String,String])

case class AnalyzeFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'COLLECT'
 * 
 ***************************************************************************/

case class StartCollect()

case class CollectFailed(data:Map[String,String])

case class CollectFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'PREPARE'
 * 
 ***************************************************************************/

/**
 * StartPrepare specifies a message sent to a data preparer actor to indicate 
 * that the preparation sub process has to be started
 */
case class StartPrepare(data:Map[String,String])

case class PrepareFailed(data:Map[String,String])

/**
 * PrepareFinished specifies a message sent to the DataPipeline actor to 
 * indicate that the data preparation sub process finished sucessfully
 */
case class PrepareFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'LEARN'
 * 
 ***************************************************************************/

case class StartLearn()

case class LearnFailed(data:Map[String,String])

case class LearnFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'ENRICH'
 * 
 ***************************************************************************/

case class StartEnrich()

case class EnrichFailed(data:Map[String,String])

case class EnrichFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'PREDICT'
 * 
 ***************************************************************************/

case class StartPredict()

case class PredictFailed(data:Map[String,String])

case class PredictFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'PROFILE'
 * 
 ***************************************************************************/

case class StartProfile()

case class ProfileFailed(data:Map[String,String])

case class ProfileFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'LOAD'
 * 
 ***************************************************************************/

case class StartLoad()

case class LoadFailed(data:Map[String,String])

case class LoadFinished(data:Map[String,String])

case class Customer(
  /* 
   * The 'apikey' of the Shopify cloud service is used as a
   * unique identifier for the respective tenant or website
   */
  site:String,
  /*
   * Unique identifier that designates a certain Shopify
   * store customer
   */
  id:String,
  /*
   * The first and last name of the customer to be used
   * when visualizing computed customer information
   */
  firstName:String,
  lastName:String,
  /*
   * THe signup date of the customer
   */
  created_at:String,
  
  /*
   * The email address of a customer and a flag to indicate,
   * whether this address is verified; this is relevant for
   * email marketing
   */
  emailAddress:String,
  emailVerified:Boolean,
  /*
   * A flag that indicates whether the customer accepts
   * marketing or not
   */
  marketing:Boolean,
  /*
   * The state of the customer, i.e. 'disabled' or 'enabled'
   */ 
  state:String

)

case class Image(
  /*
   * Unique identifier that designates a certain Shopify
   * store product image
   */
  id:String,

  position:Int,
  src:String
)

case class Location(
    
  countryname:String,
  countrycode:String,

  region:String,
  regionname:String,
  
  areacode:Int,
  dmacode:Int,
  
  metrocode:Int,
  city:String,
  
  postalcode:String,
	  
  lat:Double,
  lon:Double

)

case class Product(
  /* 
   * The 'apikey' of the Shopify cloud service is used as a
   * unique identifier for the respective tenant or website
   */
  site:String,
  /*
   * Unique identifier that designates a certain Shopify
   * store product
   */
  id:String,
  /*
   * The category assigned to a certain product, this field
   * can be used to group similar products or to determine
   * customer preferences
   */
  category:String,
  /*
   * The name of a certain product
   */
  name:String,

  vendor:String,

  /*
   * 'tags' describes a comma separated list of keywords
   * that describe a certain product
   */
  tags:String,

  images:List[Image]

)

/**
 * OrderItem is used to describe a single order or purchase
 * related entity that is indexed in an Elasticsearch index 
 * for later mining and prediction tasks
 */
case class OrderItem(
  /*
   * Unique identifier to determine a Shopify product
   */
  item:Int,
  /*
   * The name of a certain product; this information is
   * to describe the mining & prediction results more 
   * user friendly; the name is optional
   */
  name:String = "",
  /*
   * The number of items of a certain product within
   * an order; the quantity is optional; if not provided,
   * the item is considered only once in the respective
   * market basket analysis
   */
  quantity:Int = 0,
  /*
   * The category assigned to a certain product, this field
   * can be used to group similar products or to determine
   * customer preferences
   */
  category:String,  
  /*
   * The vendor of a certain item; this field and also the
   * respective category is filled by an additional request
   * to the product database
   */
  vendor:String,
  
  /*
   * Currency used with the respective order    
   */     
  currency:String = "USD",
  /*
   * Price of the item 
   */
  price:String = "",
  /*
   * SKU 
   */
  sku:String = ""
  
)
case class Order(
  /* 
   * The 'apikey' of the Shopify cloud service is used as a
   * unique identifier for the respective tenant or website
   */
  site:String,
  /*
   * Unique identifier that designates a certain Shopify
   * store customer (see ShopifyCustomer) 
   */
  user:String,
  /*
   * The IP address that is assigne to an online order;
   * this field is leveraged to determine location data
   */
  ip_address:String,
  /*
   * The user agent for the online access; this field is
   * leveraged to determine the referrer and others
   */
  user_agent:String,
  /*
   * The timestamp for a certain Shopify order
   */
  timestamp:Long,
  /*
   * The group identifier is equal to the order identifier
   * used in Shopify orders
   */
  group:String,
  /*
   * The total amount of a certain purchase or transaction
   */
  amount:Double,
  /*
   * The total discount of a certain purchase or transaction
   */
  discount:Double,
  /*
   * The total shipping costs of a certain purchase or transaction
   */
  shipping:Double,
  /*
   * The items in an order
   */ 
  items:List[OrderItem]
)
case class Orders(items:List[Order])

object ResponseStatus extends BaseStatus

object Sources {
  
  val ELASTIC:String = "ELASTIC"
    
}


object Serializer extends BaseSerializer {
  def serializeActorsStatus(statuses:ActorsStatus):String = write(statuses) 
}

object Messages extends BaseMessages
