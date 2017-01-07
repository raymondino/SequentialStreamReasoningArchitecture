package com.pnnl.shyre.utilities;


import java.time.LocalTime;
import java.util.Random;

public class GraphIdCounterPair implements Comparable<GraphIdCounterPair>{

	public String graphId;
	public LocalTime arrivalTime;
	public LocalTime expirationTime;
	
	public GraphIdCounterPair (String id, LocalTime arrivalTime_) {
		this.graphId = id;
		this.arrivalTime = arrivalTime_;		

		// assign randomly the expiration time for each graph 
		// the expiration time is after the arrival time within a range of 
		// 1 minute to 25000 seconds
		Random rn = new Random();
		expirationTime = arrivalTime.plusSeconds(rn.nextInt(25000)+1);
	}
	
	@Override
	public int compareTo(GraphIdCounterPair entry) {
		
		if(this.expirationTime == entry.expirationTime) {
			return 0;
		}
		// order the elements in the minHeap according to the expiring time
		return this.expirationTime.isBefore(entry.expirationTime) ? -1: 1; 
	}	
}
