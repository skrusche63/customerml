
## Prepare Subsystem

The *prepare* phase follows the initial *collect* phase and is the first phase of the customer 
science process defined by CustomerML.

While data collection is repeatedly applied on a weekly or monthly base, data preparation 
is an on-demand task, that should be performed if prior analysis results no longer describe
the customer behavior well enough.

The *prepare* subsystem evaluates the purchase data of a store platform under multiple 
perspectives, and therefore, comprises a set of adjusted preparers.

---

### Purchase Metrics

A purchase metrics provides a first look (or overview) into the purchase data of a certain 
time window. The metrics comprises statistical information of the monetary, time and product 
purchase dimension.

The monetary dimension covers the total amount spent by all customers, the mean, maximum and 
minimum value, and also standard deviation and variance.

The time dimension provides statistics about the time to repeat purchase, and also frequency 
distributions about the purchase day of the week, and the time of the day.

The product dimension holds the product purchase frequency distribution of a certain purchase 
time window.

The purchase metrics is a very fast mechanism to extract actionable data. It can be directly loaded
into an Elasticsearch index and contributes to a time series of statistical data, each data point 
stemming from an individual metrics.

---

### RFM Analysis

RFM analysis is a customer analysis approach based on the wide spread RFM model. RFM is short 
for R(ecency), F(requency) and M(onetary) and is an approved method to build customer types 
(segments) from the order history.

RFM analysis (or segmentation) is a must have for every store platform before starting any 
marketing or targeting campaigns.

This platform combines RFM segmentation with advanced machine learning algorithms and thus provides 
a deep insight into a store's customer base. 

---

#### Loyalty Analysis

Every retailer knows that their customer base covers those that stick around while others tend 
to quit shopping at their store. In order to target loyal customers or start win-back campaigns 
for those that fade away, it is important to measure the customers' loyalty.  

Loyalty specifies the strength of relationship between customer and retailer. A strong indicator 
for customer loyalty is the respective (re)purchasing behavior. 

Many retailers follow a simplistic one-size-fits-all approach to describe customers as active or 
inactive. Rules such as "customers who have not purchased with the last 90 days are flagged as 
inactive" are wide-spread approaches to describe loyalty. 

However, a win-back campaign after 90 days either for customers who purchase twice a year or those 
that order every week, is premature.

Our advanced loyalty modeling approach analyzes the repeat shopping behavior of every single customer 
and makes sense of their purchase habits. This lets us identify when a customer deviates or not from 
his or her normal behavior. 

Loyalty analysis results are joined with the prior RFM segmentation results, and e.g. show whether 
high value customers deviate from their normal behavior, and tend to fade off. 

This approach has been chosen as not all customers are equally valuable to a company, and the retention 
of some can be unprofitable, i.e. it is better to let those customers fade off.

---

### Affinity Analysis

Affinity analysis is a pre-processing task for persona analysis. Personas are built from the purchase 
time behavior, and also from the products, customers buy over their lifetime.

Personas based on the purchase time behavior divides the customer base into groups of customers that 
prefer similar days of the week and hours of the day when purchasing. Product based personas segments 
the customer base into groups of customers that buy similar items. 

Personas are also built from the *time to repeat purchase* and helps to segment customers due to 
their repeat purchase patterns.  

Due to this aim, affinity analysis extracts the purchase frequency distribution for every single customer 
with respect to the day of the week and the hour of the day, the purchases are performed. This distribution
is transferred into a time preference profile per customer by applying the well known TF-IDF algorithm.

Affinity analysis also looks into the time to repeat purchase distribution, and the product frequency 
distribution for every single customer and computes the respective profiles.

The customer product preference profile is a prerequisite to compute personalized product recommendations 
by leveraging collaborative filtering techniques (see Learn Subsystem).


Affinity analysis is independently performed for each customer type (see RFM analysis) and provides the 
ability to determines personas for each customer segment.

---

### Purchase State Analysis

Purchase state analysis is a pre-processing task to build an advanced purchase process model. To this end, 
the customers purchase behavior is analyzed and described as a time ordered sequence of predefined purchase 
states.

These states form a stochastic process model. This model specifies which state is most likely followed by 
which other states, and provides the ability to determine the most probable states that follow the last 
purchase state of every single customer a certain number of steps ahead.

A purchase state is built from the last amount spent and the time elapsed since the prior purchase, and also 
takes the customer type (see RFM analysis) into account. This enables to forecast the most probable next purchases
of every single customer, a certain number of steps ahead.

---

### Geospatial Analysis

Geospatial analysis looks into the IP addresses provided with every purchase order and transforms this value 
into geospatial data, covering the country, region, city and also the WGS84 coordinates associated with the 
purchase order.

The geospatial data are determined in combination with the timestamp of the respective purchase and thus 
provide a mechanism to compute the dynamic geospatial profile for every single customer.

This profile can be used to determine the most frequent geo location, or, the distance moved between two subsequent
transactions. In combination with the purchase time one can build movement profiles, determine whether purchases 
have been made within or outside working hours and more.

---

### Product Analysis

Product analysis aims to determine those products that are frequently bought together to infer relations certain
products have with other products. This insight helps to improve product placement, plan cross-selling or promotions.

Product analysis is a pre-processing task to build these product relations.

Product analysis also leverages a quantile-based algorithm to segment products with respect to their customer purchase 
frequency. Doing so, it measures both, the customer frequency of a certain product and also the purchase frequency.

This enables to also take products into account that are frequently bought only by few customers.

This part of product analysis discovers products that are top sellers, and also determines those that are flops.



