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

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.{Configuration => HConf}
import de.kp.spark.core.{Configuration => CoreConf}
import de.kp.spark.core.{Configuration => CoreConf}
import org.apache.hadoop.conf.{Configuration => HConf}

object Configuration extends CoreConf {

    /* Load configuration for router */
  val path = "application.conf"
  val config = ConfigFactory.load(path)

  override def actor:(Int,Int,Int) = {
  
    val cfg = config.getConfig("actor")

    val duration = cfg.getInt("duration")
    val retries = cfg.getInt("retries")  
    val timeout = cfg.getInt("timeout")
    
    (duration,retries,timeout)
    
  }

  override def elastic:HConf = null
  
  def heartbeat:(Int,Int) = {
  
    val cfg = config.getConfig("actor")
    
    val heartbeat = cfg.getInt("heartbeat")
    val timeout = cfg.getInt("timeout")
    
    (heartbeat,timeout)
    
  }

  def cache():Int = {
  
    val cfg = config.getConfig("cache")
    val size = cfg.getInt("size")
    
    size
    
  }
  
  def geoip:String = {
  
    val cfg = config.getConfig("geoip")
    cfg.getString("path")
    
  }
  
  override def input:List[String] = null

  override def mysql:(String,String,String,String) = null
  
  override def output:List[String] = null
  
  override def redis:(String,String) = {
  
    val cfg = config.getConfig("redis")
    
    val host = cfg.getString("host")
    val port = cfg.getString("port")
    
    (host,port)
    
  }

  override def rest():(String,Int) = {
      
    val cfg = config.getConfig("rest")
      
    val host = cfg.getString("host")
    val port = cfg.getInt("port")

    (host,port)
    
  }
  
  def shopify():(String,String,String) = {
  
    val cfg = config.getConfig("shopify")
    
    val endpoint = cfg.getString("endpoint")
    val apikey = cfg.getString("apikey")

    val password = cfg.getString("password")
    
    (endpoint,apikey,password)
    
  }
  
  def woocommerce():(String,String,String) = {
  
    val cfg = config.getConfig("woocommerce")
    
    val secret = cfg.getString("secret")
    val key    = cfg.getString("key")

    val url = cfg.getString("url")
    
    (secret,key,url)
    
  }
  
  override def spark():Map[String,String] = {
  
    val cfg = config.getConfig("spark")
    
    Map(
      "spark.executor.memory"          -> cfg.getString("spark.executor.memory"),
	  "spark.kryoserializer.buffer.mb" -> cfg.getString("spark.kryoserializer.buffer.mb")
    )

  }
  
}