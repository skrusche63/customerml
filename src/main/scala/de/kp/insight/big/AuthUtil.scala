package de.kp.insight.big
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

import org.scribe.oauth.OAuthService

import org.scribe.builder.ServiceBuilder
import org.scribe.model._

import java.io.File
import com.thoughtworks.xstream.XStream

/**
 * AuthInfo describes the authentication info
 * received from the Bigcommerce endpoint
 */
case class AuthInfo(
  /* Temporary code to exchange for an access token */
  code:String,
  /* List of authorization scopes */
  scope:String,
  /* Base path for the authorized store context, in the 
   * format: stores/{store_hash}
   */
  context:String
)

object AuthUtil {
  
  def createService(key:String,secret:String,callback:String):OAuthService = {

    val f = new File("service.xml");
    if (f.exists()) {
      return loadService()
    }

    val service = new ServiceBuilder()
                    .provider(classOf[BigOAuth2Api])
				    .apiKey(key)
				    .apiSecret(secret)
				    .callback(callback)
				    .build()
    
    saveService(service)
    return service
    
  }

  def loadService():OAuthService = {

    val file = new File("service.xml")
    
    val xstream = new XStream();
    xstream.alias("service", classOf[OAuthService])
    
    try {
        
      val xml = FileUtil.readFile(file)
      xstream.fromXML(xml).asInstanceOf[OAuthService]
    
    } catch {
      case e:Exception => null
      
    }  
    
  }

  def saveService(service:OAuthService) {
    
    val file = new File("service.xml")
    
    val xstream = new XStream()
    xstream.alias("service", classOf[OAuthService])
    
    val xml = xstream.toXML(service)
    try {
      FileUtil.saveFile(xml,file)
      
    } catch {
      case e:Exception => {/* do nothing */}
    
    }

  }

  def loadAccessToken():Token = {

    val file = new File("token.xml")
    
    val xstream = new XStream()
    xstream.alias("token", classOf[Token])
    
    try {
    
      val xml = FileUtil.readFile(file);
      xstream.fromXML(xml).asInstanceOf[Token]
      
    } catch {
      case e:Exception => null
      
    }

  }  
  
  def saveAccessToken(token:Token) {

    val file = new File("token.xml")
    
    val xstream = new XStream()
    xstream.alias("token", classOf[Token])
    
    val xml = xstream.toXML(token)

    try {
      FileUtil.saveFile(xml, file)
      
    } catch {
      case e:Exception => {/* do nothing */}
    }
    
  }
   
  def loadAuthInfo():AuthInfo = {

    val file = new File("authinfo.xml")
    
    val xstream = new XStream()
    xstream.alias("authinfo", classOf[AuthInfo])
    
    try {
    
      val xml = FileUtil.readFile(file);
      xstream.fromXML(xml).asInstanceOf[AuthInfo]
      
    } catch {
      case e:Exception => null
      
    }

  }  
 
  def saveAuthInfo(code:String,scope:String,context:String) {
   
    val authInfo = AuthInfo(code,scope,context)
    
    val file = new File("authinfo.xml")
    
    val xstream = new XStream()
    xstream.alias("authinfo", classOf[AuthInfo])
    
    val xml = xstream.toXML(authInfo)
    try {
      FileUtil.saveFile(xml,file)
      
    } catch {
      case e:Exception => {/* do nothing */}
    
    }
    
  }

}