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
import com.pnnl.shyre.stardog.cache.StardogFIFO;
import com.pnnl.shyre.stardog.cache.StardogLFU;
import com.pnnl.shyre.stardog.cache.StardogLRU;

public class Main {
	
	private static String serverURL = "snarl://localhost:5820";
	private static String username = "admin";
	private static String password = "admin";
	
	public static void main(String[] args) throws Exception {
		Map<Integer, ArrayList<Integer>> cases = new HashMap<Integer, ArrayList<Integer>> (5); // Map<cacheSize, evictionAmount>
		cases.put(10, new ArrayList<Integer>(Arrays.asList(1, 2, 5, 9)));
		cases.put(100, new ArrayList<Integer>(Arrays.asList(1, 10, 20, 50, 99)));
		cases.put(200, new ArrayList<Integer>(Arrays.asList(1, 20, 40, 100, 199)));
		cases.put(500, new ArrayList<Integer>(Arrays.asList(1, 50, 100, 250, 499)));
		cases.put(1000, new ArrayList<Integer>(Arrays.asList(1, 100, 200, 500, 999)));
		cases.put(2000, new ArrayList<Integer>(Arrays.asList(1, 200, 400, 1000, 1999)));
		
		int numberOfTriples_ = 4;
		StardogFIFO sdfifo = new StardogFIFO(numberOfTriples_, Main.serverURL, Main.username, Main.password);
		StardogLFU sdlfu = new StardogLFU(numberOfTriples_, Main.serverURL, Main.username, Main.password);
		StardogLRU sdlru = new StardogLRU(numberOfTriples_, Main.serverURL, Main.username, Main.password);
//		AGFIFO agfifo = new AGFIFO(numberOfTriples_);
		
		String backgroundOntologyPath = "files/univ-bench.owl";
		for(Map.Entry<Integer, ArrayList<Integer>> entry : cases.entrySet()) {
//			agfifo.setSize(entry.getKey());
			sdfifo.setSize(entry.getKey());
			sdlfu.setSize(entry.getKey());
			sdlru.setSize(entry.getKey());
			ArrayList<Integer> evictArray = entry.getValue();
			for(int i : evictArray){
//				agfifo.setEvictAmount(i);
				sdfifo.setEvictAmount(i);
				sdlfu.setEvictAmount(i);
				sdlru.setEvictAmount(i);
//				agfifo.run(backgroundOntologyPath);
				sdfifo.run(backgroundOntologyPath);
				sdlfu.run(backgroundOntologyPath);
				sdlru.run(backgroundOntologyPath);
			}
		}

		sdfifo.clean();
		sdlfu.clean();
		sdlru.clean();
//		agfifo.clean();
	}
}
