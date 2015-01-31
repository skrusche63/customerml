package de.kp.insight.mag
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

import org.scribe.builder.api.DefaultApi10a
import org.scribe.model.Token

import de.kp.insight.Configuration

class MagentoAPI extends DefaultApi10a {

  private val endpoint = Configuration.magento
  
  override def getAccessTokenEndpoint():String = {
    endpoint + "oauth/token"
  }
  
  override def getRequestTokenEndpoint():String = {
    endpoint + "oauth/initiate"
  }
  
  override def getAuthorizationUrl(requestToken:Token):String = {
    /* This implementation is for admin roles only */        
	endpoint + "admin/oauth_authorize?oauth_token=" + requestToken.getToken()
  }

}