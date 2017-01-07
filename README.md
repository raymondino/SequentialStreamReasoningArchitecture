# srra
Sequential Stream Reasoning Architecture


## Introduction
SSRA is proposed with the purpose of deploying [semantic importance](https://scholar.google.com/citations?view_op=view_citation&hl=en&user=d_juGHAAAAAJ&sortby=pubdate&citation_for_view=d_juGHAAAAAJ:M3NEmzRMIkIC) into typical stream reasoning systems to enable flexible window management strategies. The architecture diagram is shown in the following picture:
![alt text](http://i.imgur.com/cZjtwc5.png "SSRA archictecture diagram")
For detailed information, please refer to our paper: [Towards A Cache-Enabled, Order-Aware, Ontology-Based Stream Reasoning Framework](http://events.linkeddata.org/ldow2016/papers/LDOW2016_paper_13.pdf)

## Installation

1. AllegroGraphCache: contains the architecture implemented based on [Allegrograph](http://franz.com/). It includes an ant script that is suitable to run in your virtual server. 
2. StardogCache: contains the architecutre implemented based on [Stardog](http://stardog.com/). It includes an ant script that is suitable to run in your virtual server.
3. ssra: contains the eclipse/pom friendly source code that is good for you to import in the eclipse.
4. benchmark results: contains all the benchmark results of different cache type, streaming data configurations. 

Please click into your desired folder for more information. 

## Disclaimer
For the historical reason, this architecture is named after "Cache". Actually we should have named it after "Window" in order to align the state-of-the-art in stream reasoning. So whenever you see a "cache" in the paper or code, please see it as "window". :-)
