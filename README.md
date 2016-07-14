# CacheStreamReasoning
A stream reasoning system prototype with caching algorithms (FIFO, LRU &amp; LFU) 
This system leverages Java client API from AllegroGraph and Stardog. 
The dataset is generated from LUBM test, with a given univ-bench.owl ontology and generated ABox data. 
ABox data is partitioned into graphs, and streamed to the system. 
The query inside requires a RDFS reasoning to get the answer. 
The cache is either disk-based or memory-based, and the caching algorithm takes the natural order or semantic importance of each graph into consideration. 

Paper published: [Towards A Cache-Enabled, Order-Aware, Ontology-Based Stream Reasoning Framework](http://events.linkeddata.org/ldow2016/papers/LDOW2016_paper_13.pdf)
