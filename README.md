# TopPI
TopPI: An Efficient Algorithm for Item-Centric Mining

An item-centric pattern mining algorithm.

With TopPI, we introduce item-centric mining, a new semantics for mining long-tailed datasets. Our algorithm, TopPI, finds for each item its top-k most frequent closed itemsets. While most mining algorithms focus on the globally most frequent itemsets, TopPI guarantees that each item is represented in the results, regardless of its frequency in the database.
TopPI allows users to efficiently explore Web data, answering questions such as “what are the k most common sets of songs downloaded together with the ones of my favorite artist?”. When processing retail data consisting of 55 million supermarket receipts, TopPI finds the itemset “milk, puff pastry” that appears 10,315 times, but also “frangipane, puff pastry” and “nori seaweed, wasabi, sushi rice” that occur only 1120 and 163 times, respectively. Our experiments with analysts from the marketing department of our retail partner, demonstrate that item-centric mining discover valuable itemsets. We also show that TopPI can serve as a building-block to approximate complex itemset ranking measures such as the p-value.
Thanks to efficient enumeration and pruning strategies, TopPI avoids the search space explosion induced by mining low support itemsets. We show how TopPI can be parallelized on multi-cores and distributed on Hadoop clusters. Our experiments on datasets with different characteristics show the superior- ity of TopPI when compared to standard top-k solutions, and to Parallel FP- Growth, its closest competitor.


Reference papers:
* TopPI: An Efficient Algorithm for Item-Centric Mining
Martin Kirchgessner, Vincent Leroy, Alexandre Termier, Sihem Amer-Yahia and Marie-Christine Rousset
In Proceedings of the International Conference on Big Data Analytics and Knowledge Discovery (DaWaK), 2016

* Testing Interestingness Measures in Practice: A Large-Scale Analysis of Buying Patterns
Martin Kirchgessner, Vincent Leroy, Sihem Amer-Yahia, Shashwat Mishra and Intermarché Alimentaire International Stime
In Proceedings of the International Conference on Data Science and Advanced Analytics (DSAA), 2016


Please use [Maven](http://maven.apache.org/) to build the program. 
