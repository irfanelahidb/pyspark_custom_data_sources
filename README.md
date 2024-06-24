# pyspark_custom_data_sources

## Summary
A repo to store code related to the new PySpark custom data sources API which enables reading from custom data sources and writing to custom data sinks in Apache Spark using Python. We can use PySpark custom data sources to define custom connections to data systems and implement additional functionality, to build out reusable data sources. What's more interesting is that it gives the capability to read/write data in streaming format.

The current documentation at https://docs.databricks.com/en/pyspark/datasources.html includes examples related to streaming reads but it doesn't persist the progress. i.e. on running a streaming query reading from a custom data source again, it starts reading data from the beginning. The examples in this repo will implement a progress persistence capability which is similar to what one gets with checkpointing against Kafka and AutoLoader.
