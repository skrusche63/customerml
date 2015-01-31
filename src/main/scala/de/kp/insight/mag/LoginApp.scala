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

import java.io.File
import java.util.Scanner

import org.scribe.model.Verifier
import org.clapper.argot._

/**
 * Magento 1.7+ provides a REST API with OAuth1.0a authorization scheme;
 * in order to make the calls to this API, we use the Scribe framework 
 * from Pablo Fernandez to accomplish this task.
 * 
 * First one has to generate the REST roles and consumers in the Magento
 * dashboard, the roles are going to map the resources inside Magento that
 * this roles needs access to and te consumer is going to represent the 
 * keys to be used insight the custom app.
 * 
 * With OAuth1, one cannot circumvent the explicit login. User has to explicitly 
 * sign in and provide access to specified resources.
 */

object LoginApp {
    
  def main(args:Array[String]) {
    
    try {
      /*
       * STEP #1: Determine whether an access token
       * already exists; 
       */
      val f = new File("token.xml")
      if (f.exists()) {
        
        AuthUtil.loadAccessToken()
        println("Access token already exists. Login process is stopped here.")
      
      } else {

        println("Access token does not exist. Continue with login process.")
      
        val params = createParams(args)
        /*
         * STEP #2: Create service
         */
        val key = params("key")
        val secret = params("secret")
      
        val service = AuthUtil.createService(key, secret)
        println("Access service created and saved.")

        val in = new Scanner(System.in)

        println("--- Magento OAuth Authorization ---\n")
        println("Fetching The Request Token...")
    
        val requestToken = service.getRequestToken()
    
        println("Request token received.\n")
        println(" Go & authorize with Magento here:")
    
        val authUrl = service.getAuthorizationUrl(requestToken)
    
        println(authUrl)
        println("\nPaste the verifier here:")
        print(">> ")
        
        val verifier = new Verifier(in.nextLine())
    
        println("\nTrading the tequest token for an access token.")
    
        val accessToken = service.getAccessToken(requestToken, verifier)
        println("Access token received.")
    
        AuthUtil.saveAccessToken(accessToken)
        println("Access token successfully created and saved.")
        
      }
      
    } catch {
      case e:Exception => {
          
        println(e.getMessage) 
        sys.exit
          
      }
      
    }

  }

  protected def createParams(args:Array[String]):Map[String,String] = {

    import ArgotConverters._
     
    val parser = new ArgotParser(
      programName = "Magento Login",
      compactUsage = true,
      preUsage = Some("Version %s. Copyright (c) 2015, %s.".format("1.0","Dr. Krusche & Partner PartG"))
    )

    val key = parser.option[String](List("key"),"key","The Magento key from OAuth consumers")
    val secret = parser.option[String](List("key"),"key","The Magento secret from OAuth consumers")

    /* http://your.magentohost.com/api/rest */
    val url = parser.option[String](List("url"),"url","The Magento REST API endpoint")

    parser.parse(args)
      
    /* Validate parameters */
    if (key.hasValue == false)
      throw new Exception("Parameter 'key' is missing.")

    if (secret.hasValue == false)
      throw new Exception("Parameter 'secret' is missing.")
    
    if (url.hasValue == false)
      throw new Exception("Parameter 'url' is missing.")
    
    /* Collect parameters */
    Map(
      "key" -> key.value.get,
      "secret" -> secret.value.get,      
      "url" -> url.value.get
    )
    
  }
  
}