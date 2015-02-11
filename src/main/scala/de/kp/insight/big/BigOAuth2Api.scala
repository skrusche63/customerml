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

import java.util.regex.Matcher
import java.util.regex.Pattern

import org.scribe.builder.api.DefaultApi20

import org.scribe.exceptions.OAuthException
import org.scribe.extractors.{AccessTokenExtractor,JsonTokenExtractor}

import org.scribe.model.{OAuthConfig,OAuthConstants,OAuthRequest}
import org.scribe.model.{Token,Verb,Verifier}

import org.scribe.oauth.{OAuthService,OAuth20ServiceImpl}
import org.scribe.utils.{OAuthEncoder,Preconditions}

import de.kp.insight.Configuration

class BigOAuth2Api extends DefaultApi20 {
  
  /**
   * Access Tokens
   * 
   * To generate the required access token for user granted scopes, one needs 
   * to recieve an Auth Callback request from the login service.
   *
   * The Auth Callback contains a temporary access code which is required to 
   * generate a long-expiry token to access the API with the user’s granted 
   * permissions.
   * 
   */
  override def getAccessTokenEndpoint():String = {
    "https://login.bigcommerce.com/oauth2/token"
  }

  /**
   * Example
   * 
   * {
   *   "access_token": "g3y3ab5cctiu0edpy9n8gzl0p25og9u",
   *   "scope": "store_v2_orders",
   *   "user": {
   *     "id": 24654,
   *     "email": "merchant@mybigcommerce.com"
   *   },
   *   "context": "stores/g5cd38"
   * }   
   * 
   */
  override def getAccessTokenExtractor():AccessTokenExtractor = new JsonTokenExtractor()

  override def getAuthorizationUrl(config:OAuthConfig):String = {
    throw new Exception("This method is not implemented / used by Bigcommerce authentication.")
  }

  override def getAccessTokenVerb():Verb = {
    Verb.POST
  }

  override def createService(config:OAuthConfig):OAuthService = new BigOAuth2Service(this,config)

}

class BigOAuth2Service(api:DefaultApi20,config:OAuthConfig) extends OAuth20ServiceImpl(api,config) {

  private val authInfo = AuthUtil.loadAuthInfo
  private val CODE_VALUE = authInfo.code

  private val CONTEXT = "context"
  private val CONTEXT_VALUE = authInfo.context
  
  private val GRANT_TYPE = "grant_type"
  private val GRANT_TYPE_VALUE = "authorization_code"

  private val SCOPE = "scope"
  private val SCOPE_VALUE = authInfo.scope

  override def getAccessToken(requestToken:Token,verifier:Verifier):Token = {
    /*
     * The Bigcommerce API requires POST requests to retrieved the access token
     */
    val request = new OAuthRequest(api.getAccessTokenVerb(), api.getAccessTokenEndpoint())
    /*
     * We need to send these parameters to the Oaccess token service:
     *
     * client_id:     The Client ID for your app
     * client_secret: The Client Secret for your app
     * code:          Temporary access code
     * scope:         List of authorization scopes
     * grant_type:    Always authorization_code
     * redirect_uri:  Must be identical to your registered Auth Callback URL
     * context:	      Base path for the authorized store context, in the format: stores/{store_hash}
     * 
     */
    request.addBodyParameter(OAuthConstants.CLIENT_ID, config.getApiKey())
    request.addBodyParameter(OAuthConstants.CLIENT_SECRET, config.getApiSecret())
    
    request.addBodyParameter(OAuthConstants.CODE, CODE_VALUE)
    request.addBodyParameter(OAuthConstants.SCOPE, SCOPE_VALUE)
    
    request.addBodyParameter(GRANT_TYPE, GRANT_TYPE_VALUE)
    request.addBodyParameter(OAuthConstants.REDIRECT_URI, config.getCallback())
    
    request.addBodyParameter(CONTEXT, CONTEXT_VALUE)
 
    val response = request.send()
    api.getAccessTokenExtractor().extract(response.getBody())
  
  }
  
}
