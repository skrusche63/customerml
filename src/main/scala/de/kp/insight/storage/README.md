
## Load Subsystem

The *load* subsystem is responsible for indexing the multiple results of the customer science 
process in an Elasticsearch cluster and turns insights directly into actionable data.

The different phases of the customer science process provide their results as files leveraging 
the [Apache Parquet](http://parquet.incubator.apache.org/) format. This approach completely 
decouples the data processing layer from the respective serving layer and makes it easy to use 
distributed storage systems other than Elasticsearch.

---

The *load* subsystem provides data for the following Elasticsearch indexes:

### Customers

The **customer** index comprises a mapping for the customer base data (gathered by the *collect* 
subsystem), and additional insight-driven  mappings to store

* Customer segments,

* Geospatial profiles,

* Loyalty profiles,

* Product recommendations,

* Purchase forecasts,

* Similar customers,

and, highly aggregated customer profiles that serve as a starting point for customer specific
information retrieval.

--- 

### Orders

The **orders** index comprises a mapping for the order data (gathered by the *collect* 
subsystem), and an additional insight-driven  mapping to store

* Purchase metrics.

---

### Personas

The **personas** index comprises insight-driven mappings to store personas derived from the 
customer time-based and product-based purchase behavior:

* Product personas,

* Purchase day personas,

* Purchase hour personas,

* Time to repeat purchase personas.

---

### Products

The **products** index comprises a mapping for the product base data (gathered by the *collect* 
subsystem), and additional insight-driven mappings to store

* Customer recommendations,

* Product relations,

* Product segments,

* Similar products.

