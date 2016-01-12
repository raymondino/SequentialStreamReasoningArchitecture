/*
 * @Author: Rui Yan
 * to-dos:
 *       1. calculate the precision, recall & f-measure for each run. 
 */
package com.pnnl.shyre.main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import com.pnnl.shyre.allegrograph.cache.Cache;;

public class Main {
	
	private static String serverURL = "http://localhost:10035";
	private static String catalog = "system";
	private static String repository = "agfefo";
	private static String username = "rui";
	private static String password = "yr";
	
	public static void main(String[] args) throws Exception {
		
		// load ground truth
		HashSet<String> groundTruth = new HashSet<String>();
		BufferedReader br = new BufferedReader(
				            new FileReader("files/LUBM45groundTruth.txt"));
		String line;
		while((line = br.readLine()) != null) {
			groundTruth.add(line);
		}
		br.close();
		System.out.println("[INFO] ground truth loaded");		

		// The following cases are for benchmark
		// Map<cacheSize, evictionAmount>
		Map<Integer, ArrayList<Integer>> cases = new LinkedHashMap<> ();
		cases.put(10, new ArrayList<Integer>(Arrays.asList(2,5,7,10)));
		cases.put(100, new ArrayList<Integer>(Arrays.asList(25,50,75,100))); 
		cases.put(1000, new ArrayList<Integer>(Arrays.asList(250,500,750,1000)));

		int[] numberOfTriples_ = {1,10,100};
			
		int evictTimes = 10; 
		String backgroundOntologyPath = "files/univ-bench.owl";
		
		for(int i = 0; i < numberOfTriples_.length; ++i) {
			Cache agfefo = new Cache(numberOfTriples_[i], evictTimes, 
				    groundTruth, Main.serverURL, Main.username, Main.password, 
				    Main.catalog, Main.repository);
			
			for(Map.Entry<Integer, ArrayList<Integer>> entry:cases.entrySet()) {
				agfefo.setSize(entry.getKey());
				ArrayList<Integer> evictArray = entry.getValue();
				for(int j: evictArray) {
					agfefo.setEvictAmount(j);
					agfefo.run(backgroundOntologyPath);
				}					
			 }
			 agfefo.clean();				
		}
	}
}
