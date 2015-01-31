package de.kp.insight.big

import org.codehaus.jackson.annotate.JsonProperty
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonIgnore}

/**
 * Specification of the REST access to products
 * associated with a certain order
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class BigProducts(

  @JsonProperty("url")
  url:String,

  @JsonProperty("resource")
  resource:String

)