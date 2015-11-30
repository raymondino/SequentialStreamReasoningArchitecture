/*
 * @Author: Rui Yan
 * to-dos:
 *       1. calculate the precision, recall & f-measure for each run. 
 */
package com.pnnl.shyre.main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.pnnl.shyre.allegrograph.cache.AGFIFO;

public class Main {
	
	public static void main(String[] args) throws Exception {
		Map<Integer, ArrayList<Integer>> cases = new HashMap<Integer, ArrayList<Integer>> (5); // Map<cacheSize, evictionAmount>
		cases.put(10, new ArrayList<Integer>(Arrays.asList(2, 5, 7, 10)));
		cases.put(100, new ArrayList<Integer>(Arrays.asList(25, 50, 75, 100)));
		cases.put(200, new ArrayList<Integer>(Arrays.asList(50, 100, 150, 200)));
		cases.put(500, new ArrayList<Integer>(Arrays.asList(125, 250, 375, 500)));
		cases.put(1000, new ArrayList<Integer>(Arrays.asList(250, 500, 750, 1000)));
		cases.put(2000, new ArrayList<Integer>(Arrays.asList(500, 1000, 1500, 2000)));
		

		int evictTimes = 20;
		String backgroundOntologyPath = "files/univ-bench.owl";

		// number of triples in a graph ranging from 1 to 100
		for(int numberOfTriples_ = 1; numberOfTriples_ < 101; numberOfTriples_*=10) {
			AGFIFO agfifo = new AGFIFO(numberOfTriples_, evictTimes);
			
			for(Map.Entry<Integer, ArrayList<Integer>> entry : cases.entrySet()) {
				agfifo.setSize(entry.getKey());

				ArrayList<Integer> evictArray = entry.getValue();
				for(int i : evictArray){
					agfifo.setEvictAmount(i);
					
					agfifo.run(backgroundOntologyPath);

				}
			}
			agfifo.clean();
		}
	}
}
