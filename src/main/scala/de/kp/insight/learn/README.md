
## Learn Subsystem

The *learn* subsystem leverages selected engines of [Predictiveworks](http://predictiveworks.eu) 
to apply machine learning algorithms to the customer purchase behavior of a certain period of time.

The *learn phase* is the second phase of the customer science process defined by CustomerML, and
follows the *prepare phase*.

### Customer Persona Model

CustomerML utilizes Predictiveworks' [Similarity Analysis](https://github.com/skrusche63/spark-cluster) engine 
to cluster customers due to their purchase *time* and *product* affinities.

The persona models can be built for every individual customer segment determined by prior RFM analysis. This implies 
that time-based or product based personas are built from the purchase behavior of customer that refer to a certain customer 
type, and thus avoids e.g. mixing up personas for high-value customers with those that have only purchased once.

### Customer Preference Model

CustomerML utilizes collaborative filtering to build customer product preference models.

The preference models can be built for every individual customer segment determined by prior RFM analysis. This implies 
that customer preferences are built from the purchase behavior of customer that refer to a certain customer type, and thus 
avoids e.g. mixing up preferences for high-value customers with those that have only purchased once.


### Product Relation Model

CustomerML utilizes Predictiveworks' [Association Analysis](https://github.com/skrusche63/spark-arules) engine
to discover association rules from those products that are frequently bought together.

The product relation models can be built for every individual customer segment determined by prior RFM analysis. This implies 
that association rules are built from the purchase behavior of customer that refer to a certain customer type, and thus 
avoids e.g. mixing up product relations for high-value customers with those that have only purchased once.


### Stochastic Purchase Model

CustomerML utilizes Predictiveworks' [Intent Recognition](https://github.com/skrusche63/spark-intent) engine
to build a stochastic purchase model from the prior discovered customer purchase states.

The purchase model specifies which purchase states are most likely followed by which other states.

The purchase models can be built for every individual customer segment determined by prior RFM analysis. This implies 
that stochastic purchase processes are built from the purchase behavior of customer that refer to a certain customer type, 
and thus avoids e.g. mixing up processes for high-value customers with those that have only purchased once.
