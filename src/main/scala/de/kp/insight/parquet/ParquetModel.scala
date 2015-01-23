package de.kp.insight.parquet
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
 * ParquetPRM is a data structure that specifies product relation
 * rules that form the basis for cross-selling, promotions etc
 */
case class ParquetPRM(
  antecedent:Seq[Int],
  consequent:Seq[Int],
  support:Int,
  total:Long,
  confidence:Double
)
/**
 * ParquetCPF is a data structure that specifies the customer-specific
 * purchase forecasts
 */
case class ParquetCPF(
  site:String,
  user:String,
  
  step:Int,
  
  amount:Double,
  time:Double,
  /* 
   * The purchase forecast can also be specified in terms of the
   * R(ecency) and M(onetary) segment the customer is likely to be
   */
  rm_state:String,
  score:Double
)
/**
 * ParquetURM is a data structure that specifies customer-specific
 * product recommendations derived from association rules and the
 * last transaction of the customer
 */
case class ParquetURM(
  site:String,
  user:String,
  recommendations:Seq[(Seq[Int],Double)]
)

/**********************************************************************
 *      
 *                       SUB PROCESS 'PREPARE'
 * 
 *********************************************************************/
/**
 * ParquetDPS is a data structure that specifies a discount, price 
 * and shipping sensitivity table
 */
case class ParquetDPS(
  site:String,
  user:String,

  mean_amount:Double,
  mval:Int,
  
  discount_ratio:Double,
  dval:Int,
  
  shipping_ratio:Double,
  sval:Int
)
/**
 * ParquetRFM is a data structure that specifies a marketing RFM table
 */
case class ParquetRFM(
  site:String,
  user:String,

  today:Long,
  /*
   * recency, frequency and monetary describe the original
   * values extracted from the orders under consideration
   */
  recency:Int,
  frequency:Int,
  monetary:Double,
  /*
   * rval, fval and mval specifies the original values with
   * respect to a quantiles (5) distribution, where the vals
   * are between 1..5, and 5 indicates the highest value for
   * the respective business company
   */
  rval:Int,  
  fval:Int,  
  mval:Int,
  /*
   * rfm_type divides the customer RFM space into 8 different
   * customer categories, 1..8, where 1 indicates the most
   * valuable customer for the respective business company.
   * 
   * The rfm_type is derived from the average values for R, F
   * and M and assigned the state, H or L, if the respective
   * value is above or below the average value
   */
  rfm_type:Int
)
/**
 * ParquetCST is a data structure that specifies a Parquet
 * table that assigns customers to a certain customer type.
 * 
 * The customer type (1..8) is the main segmentation mechanism
 * and used to apply data mining and model building to datasets
 * for certain customer types. E.g. '1' specifies the most
 * valuable customer type, and purchase forecast modeling is 
 * performed with respect to these different types.
 * 
 * It makes definitely no sense to e.g. build a state transition
 * model for customers that exist in completely different sub
 * spaces of the RFM space. 
 */
case class ParquetCST(
  site:String,
  user:String,
  rfm_type:Int
)
/**
 * ParquetLOC is a data structure tha specifies a Parquet table
 * that describes geospatial data assigned to a certain purchase
 * timestamp; this enables the creation of customer movement
 * profiles
 */
case class ParquetLOC(
  site:String,
  user:String,
  
  ip_address:String,
  timestamp:Long,
    
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

/**
 * ParquetPOM is a data structure that specifies a Parquet
 * file that describes the purchase overview of a certain
 * period of time
 */
case class ParquetPOM(
  
  total_orders:Long,
  
  /********** AMOUNT DIMENSION **********/
  
  total_amount:Double,
  total_avg_amount:Double,

  total_max_amount:Double,
  total_min_amount:Double,

  total_stdev_amount:Double,
  total_variance_amount:Double,
  
  /********** TEMPORARL DIMENSION **********/
 
  total_avg_timespan:Double,
  total_max_timespan:Double,

  total_min_timespan:Double,
  total_stdev_timespan:Double,

  total_variance_timespan:Double,

  total_day_supp:Seq[(Int,Int)],
  total_time_supp:Seq[(Int,Int)],
  
  /********** PRODUCT DIMENSION **********/

  total_item_supp:Seq[(Int,Int)],
  total_items:Long,
  
  /********** CUSTOMER DIMENSION **********/
  
  total_customers:Long
  
)

/**
 * ParquetPPF is a data structure that specifies a Parquet
 * file that assigns frequencies to a certain product or 
 * item. The data record can be computed with respect to
 * a certain customer type (rfm_type)
 */
case class ParquetPPF(
  item:Int,
  /* The customer and purchase frequency */
  customer:Int,
  purchase:Int,
  /*
   * cval and pval speciy the original values with respect 
   * to a quantiles (5) distribution, where the vals are 
   * between 1..5, and 5 indicates the highest value for
   * the respective business company
   */
  cval:Int,
  pval:Int
)
/**
 * ParquetCTA is a data structure that specifies a Parquet
 * file that describes the customer's affinity to the day
 * of the week, the hour of the day and also the timespan
 * between two subsequent transactions; all these features
 * specifiy the temporal affinity of a certain user.
 * 
 * This data structure is shared with Predictiveworks' vector 
 * analysis engines, such as Outlier or Similarity analysis.
 * 
 * TODO: Integrate Jollydays project and defines calendars
 * for holidays 
 * 
 */
case class ParquetCTA(
  site:String,
  user:String,
  
  item:Int,
  
  row:Long,
  col:Long,

  label:String,
  value:Double
)

/**
 * ParquetCPA is a data structure that specifies a Parquet
 * file that describes the customer product affinity; this
 * data structure is shared with Predictiveworks' vector
 * analysis engines, such as Outlier or Similarity analysis 
 */
case class ParquetCPA(
  site:String,
  user:String,
  
  item:Int,
  
  row:Long,
  col:Long,

  label:String,
  value:Double
)
/**
 * ParquetASR is a data structure that is shared with Predictiveworks'
 * Association Analysis engine; it is generated by the ASRPreparer and
 * used by the Association Rule Mining algorithm
 */
case class ParquetASR(
    site:String,
    user:String,
    timestamp:Long,
    group:String,
    item:Int
)
/**
 * ParquetCPS is a data structure that is shared with Predictiveworks'
 * Intent Recognition engine; it is generated by the STMPreparer and 
 * used by the Markov and Hidden Markov algorithm
 */
case class ParquetCPS(
  site:String,
  user:String,
  
  amount:Double,
  timestamp:Long,
  
  /*
   * We need to provide the quantile boundaries for amount ratio
   * and timespan with the customer specific state description 
   * in order to re-interpret the predicted states in terms of
   * amounts and timespans
   */
  r_b1:Double,
  r_b2:Double,
  r_b3:Double,
  r_b4:Double,
  r_b5:Double,
  
  s_b1:Double,
  s_b2:Double,
  s_b3:Double,
  s_b4:Double,
  s_b5:Double,
  
  /*
   * This is the state specification of the respective customer
   * amount and timespan; note, that the state refers to the
   * timestamp above
   */
  state:String

) 
/**
 * ParquetCLS is a data structure that specifies a Parquet file
 * that describes the loyalty segmentation based on the repeat
 * purchase behavior ofthe customer base
 */
case class ParquetCLS(
  site:String,
  user:String,
  
  amount:Double,
  timespan:Int,
  
  loyalty:Int,
  rfm_type:Int
)

/**********************************************************************
 *      
 *                       SUB PROCESS 'ENRICH'
 * 
 *********************************************************************/

/**
 * PaquetCCS is a data structure that specifies a Parquet file
 * that describes users that are similar to a certain user
 */
case class ParquetCCS(
  site:String,
  user:String,
  other:String,
  score:Double
)
/**
 * PaquetCPD is a data structure that specifies a Parquet file
 * that describes the distance of a certain customer to a specific
 * persona
 */
case class ParquetCPD(
  site:String,
  user:String,
  persona:Int,
  distance:Double
)
/**
 * PaquetCPR is a data structure that specifies a Parquet file
 * that describes user-item recommendations; this format is used
 * for user -> item as well as for item -> user assignments 
 */
case class ParquetCPR(
  site:String,
  user:String,
  item:Int,
  score:Double
)
/**
 * ParquetPSA is a data structure that specifies a Parquet file
 * that describes personas derived from a certain clustering or
 * Similarity Analysis result
 */
case class ParquetPSA(
  cluster:Int,
  item:Int,
  label:String,
  value:Double
)
/**
 * PaquetPPS is a data structure that specifies a Parquet file
 * that describes products that are similar to a certain product
 */
case class ParquetPPS(
  site:String,
  item:Int,
  other:Int,
  score:Double
)

/**********************************************************************
 *      
 *                       SUB PROCESS 'PROFILE'
 * 
 *********************************************************************/

case class ParquetCPP(
  site:String,
  user:String,
  /*
   * recency, frequency and monetary describe the original
   * values extracted from the orders under consideration
   */
  recency:Int,
  frequency:Int,
  monetary:Double,
  /*
   * rval, fval and mval specifies the original values with
   * respect to a quantiles (5) distribution, where the vals
   * are between 1..5, and 5 indicates the highest value for
   * the respective business company
   */
  rval:Int,  
  fval:Int,  
  mval:Int,
  /*
   * lval specifies the customer's loyalty segment
   */
  lval:Int,
  
  /*
   * rfm_type divides the customer RFM space into 8 different
   * customer categories, 1..8, where 1 indicates the most
   * valuable customer for the respective business company.
   * 
   * The rfm_type is derived from the average values for R, F
   * and M and assigned the state, H or L, if the respective
   * value is above or below the average value
   */
  rfm_type:Int,  
  /* Description of the day cluster */
  d_type:Int,
  d_distance:Double,
  /* Description of the hour cluster */
  h_type:Int,
  h_distance:Double,
  /* Description of the timespan (recency) cluster */
  r_type:Int,
  r_distance:Double,
  /* Description of the product cluster */
  p_type:Int,
  p_distance:Double
)