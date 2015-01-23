
## Enrich subsystem

The *enrich phase* is the third phase of the customer science process defined by CustomerML, and
follows the *learn phase*.


### Personas

In this phase, the persona models learned from the customers' purchase *time* and *product* affinities 
are used to describe the multiple personas in terms of their characteristic features.

E.g for the different product-based personas, the available products are used to specify their affinity 
to the respective persona. The detected time-based personas are described in an equivalent manner.

As a second step, the most likely persona for each and every customer is identified and assigned to 
the respective customer. This allows marketing teams to target customers with the right content at the 
time.


### Recommendations

CustomerML evaluates the customer preference model learned from the customer product preferences 
and predicts a number of most preferred products to every single customers. These predictions form a 
pre-computed basis for product recommendations (*recommended for you*).

CustomerML also evaluates the customer preference model to predict a number of most similar products 
for every single product. These predictions form a pre-computed basis for product recommendations (*more like this*).


### Purchase Forecasts

CustomerML evaluates the stochastic process model learned from all the purchase states of customers of 
a certain customer segment and forecasts for each and every customer the next purchase amount and time a 
number of steps ahead in the future.