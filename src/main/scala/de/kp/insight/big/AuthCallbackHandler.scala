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

/**
 * The Auth Callback URI is the URI within your app that accepts 
 * an OAuth2 callback request. This URI is provided through the app 
 * registration process. 
 * 
 * When a merchant installs an app for the first time, they are redirected 
 * to the app’s Auth Callback URI.
 *
 * Auth Callback URIs must accept a GET request, which includes a temporary 
 * code to exchange for an access token and the list of scopes which the user 
 * has granted access to.
 * 
 * The AuthCallbackHandler must be used within a HTTPS server that accepts 
 * GET requests, and is responsible for saving these tempory AUTH data.
 * 
 * 
 */
class AuthCallbackHandler {

  def save(code:String,scope:String,context:String) {
    AuthUtil.saveAuthInfo(code, scope, context)
  
  }
  
}