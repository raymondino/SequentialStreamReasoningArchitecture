# Architecture Benchmark Results
This folder contains the SSRA benchmark results. 

The architecture has been implemented by two off-the-shelf triplestores, [Stardog](http://stardog.com/) and [Allegrograph](http://franz.com/). 

The goal of this architecture is to provide empirical and experimental evidence to 

1. build a stream reasoning system
2. explore how semantic importance can influence the streaming window management
3. validate the hypothesis that different window management strategies employed in the window can yield different results

Two type of triplestores have been tested, namely memory-based and disk-based triplestores. 

We have configured streaming data to be **x** triples in one graph, window size to by **y** graphs, window evicts **z** graphs each time.

We also configured three window management strategies: **FEFO** first expire first out. **LFU** least frequently used. **LRU** least recently used. 

The bench mark result form, for example agFEFO-disk_bench_1_10_2, means Allegrograph disk based triplestore, with streaming data as 1 (x=1) triple in one graph, 10 (y=10) graphs as the window size, 2 (z=2) graphs are evicted each time. 


The folder "result visualization" provides interactive visualization html files that can be open in your browser to visualize some results. 
