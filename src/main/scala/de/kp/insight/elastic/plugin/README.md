
## Elasticsearch Predictive Plugin

The final results of the multiple phases of the customer science process are stored in an Elasticsearch 
cluster, extending the available customer, product & order data. The benefit of this approach is, that all
insights from the customers' purchase behavior are searchable and can be directly transformed into valuable 
actions.

And, as all insights derived from the customer science process are time-stamped data, it is easy to visualize 
these data with [Kibana](http://www.elasticsearch.org/overview/kibana/), and, e.g. compare insights that have 
different timestamps.

CustomerML also provides a plugin for Elasticsearch (Predictive Plugin) to transform this search engine into
a recommender system. The following recommendation types are supported:


### Recommended for you

This type of recommendation provides a pre-computed set of products that are characterized by the highest 
affinity to a certain customer with respect to all other products available.

The recommendations are derived by applying collaborative filtering to all preference profiles for customers 
that are assigned to the same customer segment or type. This approach avoids mining up profiles for high-value 
customers with those that e.g. have only purchased once.

This ensures that these product recommendations are as personalized as possible for a specific customer.


### More like this

This recommendation type does not take an individual customer into account, but has a strong focus on a certain 
product, and delivers those (other) products that are most similar to the selected one.

The most similar products are derived from the collaborative filtering results by computing the cosine similarity
of all products in the latent feature vector space. The data used by collaborative filtering refer to a certain 
customer segment or type, and, this implies that the most similar products also computed for a specific segment.

This aspect refers to a more general data strategy that is applied by the customer science process. 


### Bought also bought

This type of recommendation is also independent of the individual customer and has a strong focus on products 
that are related to a certain set of other products. The product relations this recommendation type is based on,
are discovered from products that are frequently bought together by customer of a certain customer segment 


This kind of recommendation provides products that are related to a certain product or a set of 
products. 


### Top selling products

Topic selling products do not refer to a certain customer, and are retrieved from product purchase frequencies.

These frequencies exist for two different contexts: one is independent of a certain customer segment 
and retrieves the top sellers from the **purchase metrics**, based on the purchase data of a certain time 
window, and derived in the data preparation phase (see Prepare Subsystem).

The other context takes a certain customer segment (e.g. frequent buyer) into account and retrieves the 
top sellers from the **product purchase frequency** of the respective segment. By using product frequencies
in this context, the recommendation request must specify a certain customer type (or segment).

This information can be retrieved from multiple mappings within the *customer* index. An appropriate one, that 
is recommended here, is the *segments* mapping, i.e ```customers/segments```. 

