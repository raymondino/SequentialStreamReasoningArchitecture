/*
 * @Author: Rui Yan
 * Usage: to facilitate eviction. 
 */

package com.pnnl.shyre.utilities;

import java.time.LocalTime;
import java.util.Random;

public class GraphIdCounterPair implements Comparable<GraphIdCounterPair>{
	public String graphId;
	public int count;
	public String mode;
	public LocalTime arrivalTime;
	public LocalTime expirationTime;
	
	// this constructor is for FEFO
	public GraphIdCounterPair (String id, int count, 
			LocalTime arrivalTime_, String mode_) {
		this.graphId = id;
		this.count = count;
		this.arrivalTime = arrivalTime_;		

		// assign randomly the expiration time for each graph 
		// the expiration time is after the arrival time within a range of 
		// 1 minute to 2500 seconds
		Random rn = new Random();
		expirationTime = arrivalTime.plusSeconds(rn.nextInt(2500)+1);
		
		// specifying FEFO, LRU & LFU
		this.mode = mode_;
	}
	
	// this constructor is for LRU & LFU
	public GraphIdCounterPair (String id, int count, 
			LocalTime arrivalTime_, LocalTime expirationTime_, String mode_) {
		this.graphId = id;
		this.count = count;
		this.arrivalTime = arrivalTime_;	
		this.expirationTime = expirationTime_;
		this.mode = mode_;
	}
	
	@Override
	public int compareTo(GraphIdCounterPair entry) {
		
		// order in LFU
		if(this.mode.equals("LFU")) { 
			if(this.count == entry.count) {
				return 0;
			}
			// order the elements in the minHeap in an ascending order	
			return this.count < entry.count ? -1: 1; 
		}
		// order in LRU
		else if(this.mode.equals("LRU")) {
			if(this.arrivalTime == entry.arrivalTime) {
				return 0;
			}
			// order the elements in the minHeap according to the arrival time
			return this.arrivalTime.isBefore(entry.arrivalTime) ? -1: 1; 
		}
		// order in FEFO
		else {
			if(this.expirationTime == entry.expirationTime) {
				return 0;
			}
			// order the elements in the minHeap according to the expiring time
			return this.expirationTime.isBefore(entry.expirationTime) ? -1: 1; 
		}
		
	}	
	
}
