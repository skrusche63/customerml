package de.kp.insight
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

import org.joda.time.format.DateTimeFormat

object DateUtil {

  val DEFAULT:String = "default"
  private val DEFAULT_FORMAT = "yyyy-MM-dd HH:mm"

  /*
   * Wed, 14 Nov 2012 19:26:23 +0000
   */
  val BIG_COMMERCE:String = "bigcommerce"
  private val BIGCOMMERCE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss Z"
  
  /*
   * 2008-12-31 03:00
   */
  val SHOPIFY:String = "shopify"
  private val SHOPIFY_FORMAT = "yyyy-MM-dd HH:mm"
  
  private val mapper = Map(

    DEFAULT -> DEFAULT_FORMAT,
    
    BIG_COMMERCE -> BIGCOMMERCE_FORMAT,
    SHOPIFY      -> SHOPIFY_FORMAT
  
  )
  
  def formatted(time:Long,category:String):String = {

    val pattern = mapper(category)
    val formatter = DateTimeFormat.forPattern(pattern)
    
    formatter.print(time)
    
  }
  
  /*
   * The common datetime input format is
   *  
   * 2008-12-31 03:00
   * 
   * This format is transformed into the
   * respective timestamp format
   * 
   */
  def unformatted(date:String,category:String):Long = {

    val pattern = mapper(category)
    val formatter = DateTimeFormat.forPattern(pattern)
 
    formatter.parseMillis(date)
    
  }

}