Spark Skew Join Examples
___

Examples on handling skewed data in Spark, with strategies to remove skews while doing joins.
In the first version these examples are geared towards data sets where the value of the skew is known. For data sets where skews are unknown, check out https://github.com/anish749/spark-skew-join.

### Data Sources used:
 - NYSE stock trading data from Kaggle.
 
## Demo:

### Compiling and running:
##### Versions used and tested:
 - Scala 2.11.6
 - Spark 2.1.0
 - Java 1.8 and Maven 3.3.9

 
##### Download data from:
 - [NYSE Stocks](https://www.kaggle.com/dgawlik/nyse) NYSE stock trading dataset.
 
Compile as follows
```
mvn clean package
```

Load the project in IntelliJ for analysis, and understanding. Since this is meant to be more of examples, I didn't try in a Sandbox or in a cluster. However things would work just fine with correct paths.

Run the class ```CreateRandomSkewedData```. This would generate some skewed data which we would use for our understanding. This also modifies the data to increase the skew for our examples.

### Overview of Data
Once we run the ```CreateRandomSkewedData``` we would see 5 data sets generated in skewed_stock folder.

##### fundamentals
This is a small dataset with only 275 records. This is the stock symbol and some look up date. We would use this to understand broadcast join.

##### prices_raw
The dataset we downloaded is converted by taking on the first two letters of the symbol. This effectively increases skew a bit. But enough to have one particular stock with a very high frequency of records.
We have this stored as 4 nearly equally sized partitions.

##### prices_withFeatures
This is meant to look like some processed data and we add a few other columns f1, f2... f5. We have also multiplied one record (for symbol AB) by 3e6 which means a heavy skew for one particular value.
Real world datasets look similar to this with a heavy skew on one or two values and moderate to no skews for all others.

##### someCalculatedLargeData
Supposed to be the other side of joins, this is an aggregated data at a day level for each symbol. This dataset has no skew. Partitioned based on the symbol.
 
##### someCalculatedLargeData_withdups
The previous dataset but with an added skew on a different value than the one with prices_withFeatures. This is supposed to be used as the right side of the join, but with a different skewed value than the left side. This prevents a pure cross join.

### Executing the examples
The examples for the joins are in the following two classes.

##### IsolatedSkewJoin
This is meant for joining two large (none fits in memory of a single node) with one of the values in one data frame having a heavy skew.
This is a very common scenario. Execute the code after generating the datasets and use the Spark UI to see the task times.

Since we know the value on which the skew exists, we handle it differently by using a technique called Fragmented-Replicated join.
The dataset which has the skew is fragmented in isolation by introducing a salt. The same record in the other dataset is replicated to match each value of the salt. The join is then made with both the actual key and the salt as the new key.

I will write a detailed explanation in a blog post soon.

##### SkewedBroadcastJoin
In this case one of the datasets is small enough to completely fit in memory and can be broad casted across all executors, resulting in a map side join.
The other dataset is skewed. We introduce a salt for isolating the skewed value and fragmenting it, and then repartitioning the dataframe with the original key and the salt to divide the skew into multiple smaller partitions.
The join then proceeds as usual. Because the data has been repartitioned, the skew has been removed.


### Dataset Utils
A few utility functions used for understanding partition stats and skews are provided in the Utils class and can be used by importing accordingly.

___

