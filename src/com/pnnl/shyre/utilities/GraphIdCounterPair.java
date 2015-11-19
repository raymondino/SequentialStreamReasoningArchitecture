package com.pnnl.shyre.utilities;

import java.time.LocalTime;

public class GraphIdCounterPair implements Comparable<GraphIdCounterPair>{
	public String graphId;
	public int count;
	public LocalTime time;
	
	public GraphIdCounterPair (String id, int count,LocalTime timestamp) {
		this.graphId = id;
		this.count = count;
		this.time = timestamp;
	}
	
	@Override
	public int compareTo(GraphIdCounterPair entry) {
		if(this.count != -1 && entry.count != -1) { // to differtiate whether it's counter-based or time-based ranking
													// set count = -1 when using LRU.
			if(this.count == entry.count) {
				return 0;
			}
			return this.count < entry.count ? -1: 1; // order the elements in the minHeap in an ascending order	
		}
		else {
			if(this.time == entry.time) {
				return 0;
			}
			return this.time.isBefore(entry.time) ? -1: 1; // order the elements in the minHeap in an old-to-new order
		}
		
	}	
	
}
