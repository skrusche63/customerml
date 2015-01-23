![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/customerml/master/images/dr_kruscheundpartner_640.png)

---

![CustomerML](https://raw.github.com/skrusche63/customerml/master/images/customerml_320.png)

CustomerML is a customer science platform that leverages predictive analytics to achieve a deep customer understanding,
learn preferences and forecast future behavior. 

It is based on the [Predictiveworks](http://predictiveworks.eu) and uses approved machine learning algorithms to recommend 
preferred products to customers, predict the next probable purchases, identify customers that look alike others, and more.

CustomerML starts from the approved RFM segmentation concept (see below) to group customers by their business value, and combines this 
approach with state-of-the-art predictive analytics.

 
> CustomerML empowers e-commerce solution partners to add the *missing* customer perspective to modern e-commerce 
platforms, such as Magento, Demandware, Hybris, Shopify and others, and enables those platforms to target the right 
customer with the right product at the right time.

---

### Customer Science & Search

Modern search engines are one of the most commonly used tools to interact with data volumes at any scale, and, Elasticsearch 
is one of the most powerful distributed and scalable member of these engines.

CustomerML performs two consecutive steps to empower e-commerce platforms with actionable customer insights:

First, CustomerML connects to a store platform, collects customer, product and order data on a weekly (or monthly basis), loads 
these data into an Elasticsearch cluster, and makes these data searchable from the very beginning.

Second, CustomerML applies a well-defined customer science process to these data, and determines advanced customer profiles, 
purchase forecasts, product recommendations, purchase frequencies and more. These insights are persisted by leveraging Apache's 
Parquet format, and are also loaded to the Elasticsearch cluster, thereby extending the existing e-commerce data.

CustomerML uses Elasticsearch as the common serving layer for all kinds of data, makes them directly searchable and 
accessible by other search-driven applications through Elasticsearch's REST API.

And, with [Kibana](http://www.elasticsearch.org/overview/kibana/), the results of the customer science process can be 
directly visualized or time-based comparisons can be made with prior results.  
 

---

### Features

##### Cohort Analysis

Cohort analysis determines how different groups (cohorts) of customers behave over time. This provides a clear
insight into customer retention trends and an understanding into business health. 

CustomerML delivers and stores all customer insights as time-stamped data in an Elasticsearch cluster. This 
approach converts cohort analysis into simply defining business specific search templates that can be directly 
visualized by leveraging [Kibana](http://www.elasticsearch.org/overview/kibana/).


##### Geospatial Analysis

Geospatial analysis looks into the IP addresses provided with every purchase order and transforms this value 
into geospatial data, covering the country, region, city and also the WGS84 coordinates associated with the 
purchase order.

The geospatial data are determined in combination with the timestamp of the respective purchase and thus 
provide a mechanism to compute the dynamic geospatial profile for every single customer.

This profile can be used to determine the most frequent geo location, or, the distance moved between two subsequent
transactions. In combination with the purchase time one can build movement profiles, determine whether purchases 
have been made within or outside working hours and more.


##### Loyalty Analysis

Every retailer knows that their customer base covers those that stick around while others tend to quit shopping at their store. 
In order to target loyal customers or start win-back campaigns for those that fade away, it is important to measure the customers' 
loyalty.  

A strong indicator for customer loyalty is the respective (re)purchasing behavior. 

CustomerML provides a personalized modeling approach and analyzes the customers' repeat shopping behavior, makes sense of 
their purchase habits, and, identifies when a customer deviates or not from his or her normal behavior. 

Loyalty analysis results are joined with the prior RFM segmentation results, and e.g. show whether high value customers deviate 
from their normal behavior, and tend to fade off.


##### Persona Analysis

Shopify builds personas from the purchase time behavior, and also from the products, customers buy over their lifetime. 
Personas specify customers with similar purchase behavior.

Personas based on the purchase time behavior divides the customer base into groups of customers that prefer similar 
days of the week and hours of the day when purchasing. Product based personas segments the customer base into groups 
of customers that buy similar items. 

Personas are also built from the *time to repeat purchase* and helps to segment customers due to their repeat purchase 
patterns.  


##### Product Recommendations

CustomerML analyzes the purchase behavior of every single customer and computes product preference profiles describing
the affinity a certain customer has with a specific product.

CustomerML uses these individual preference profiles and trains a predictive model. This model is capable to predict the 
customer product preferences, even to those products that have not been purchased yet.  

CustomerML leverages this predictive model to compute product recommendations for every single customer based on the products 
with the highest preferences.


##### Product Analysis

Product (affinity) analysis determines those products that are frequently bought together to detect latent product
relations. This helps to improve product placement, to plan cross-selling or promotions.

Product analysis also leverages a quantile-based algorithm to segment products with respect to their customer purchase 
frequency. This part of product analysis discovers products that are top sellers, and also determines those that are flops, 
distinguished by high-value and less-value customer segments. 


##### Purchase Forecast

CustomerML analyzes the customers' purchase behavior and describes it as a time ordered sequence of purchase states. 
From these states, an advanced stochastic purchase model is built, to specify which purchase states are most likely followed 
by which other states.

Based on this purchase model, the most probable states are predicted, that follow the last purchase state of every single 
customer, a number of steps ahead. From this result, the next most probable purchase times, associated with the most likely 
amount of money spent, are derived. 


##### Purchase Metrics

A purchase metrics provides a first look (or overview) into the purchase data of a certain time window. The metrics comprises 
statistical information of the monetary, time and product purchase dimension.

The purchase metrics is a very fast mechanism to extract actionable data, and, loaded into an Elasticsearch cluster, contributes 
to a time series of statistical data, each data point stemming from an individual metrics.


##### RFM Analysis

The concept of RFM has proven very effective when applied to marketing databases and describes a method 
used for analyzing the customer value.

RFM analysis depends on R(ecency), F(requency) and M(onetary) measures which are 3 important purchase 
related variables that influence the future purchase possibilities of customers.

*Recency* refers to the interval between the time, the last consuming behavior happens, and present. The importance 
of this parameters results from the experience that most recent purchasers are more likely to purchase again than 
less recent purchasers. 

*Frequency* is the number of transactions that a customer has made within a certain time window. This measure is used 
based on the experience that customers with more purchases are more likely to buy products than customers with fewer 
purchases. 

*Monetary* refers to the cumulative total of money spent by a particular customer. 

RFM provides a simple yet powerful framework for quantifying customer purchase behavior and is an excellent means to 
segment the customer base. Example: A customer has made a high number of purchases with high monetary value but not for 
a long time. At this situation something might have gone wrong, and marketers can contact with this customer to get feedback, 
or start a reactivation program.

CustomerML utilizes the approved RFM concept to segment customers by their business value, and combines this 
approach with modern data mining and predictive analytics.


##### RFM Forecast

The results of RFM analysis provide a *snapshot* of the business performance in a certain period of time and at a specific 
timestamp (when the analysis has been made).

CustomerML applies machine learning algorithms to these time-stamped RFM data and computes forecasts for the recency, 
frequency and monetary parameters for the next days, weeks, months or years.





