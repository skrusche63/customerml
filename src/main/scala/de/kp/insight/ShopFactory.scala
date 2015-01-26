package de.kp.insight

import de.kp.insight.shopify.ShopifyContext
import de.kp.insight.woo.WooContext

object ShopFactory {

  /* Support shop platforms */
  val SHOPIFY:String = "shopify"
  val WOO_COMMERCE:String = "woocommerce"
  
  val shops = List(SHOPIFY,WOO_COMMERCE)
  
  def get(ctx:RequestContext,shop:String):ShopContext = {

    shop match {
    
      case SHOPIFY => new ShopifyContext(ctx)
      
      case WOO_COMMERCE => new WooContext(ctx)
      
      case _ => throw new Exception("The shop platform '" + shop + "' is actually not supported.")
    
    }
      
  }
  
}