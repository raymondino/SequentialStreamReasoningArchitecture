package launcher;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.LinkedHashMap;

import stardog.StardogCache;
import allegrograph.AGCache;


public class Launcher {
	private static String stardogServerURL = "http://localhost:5820";
	private static String stardogUsername = "admin";
	private static String stardogPassword = "admin";
	private static String agraphServerURL = "http://localhost:10035";
	private static String agraphCatalog = "system";
	private static String agraphRepository = "agfefo";
	private static String agraphUsername = "admin";
	private static String agraphPassword = "admin";
	
	public static void main(String[] args) throws Exception {
		if(args.length !=  1) {
			System.out.println("wrong input argument, please specify either 'stardog' or 'agraph'");
		}
		if(args[0].equals("stardog")) { runStardog(); }
		else if (args[0].equals("agraph")){ runAgraph(); }
		else { System.out.println("wrong input argument, please specify either 'stardog' or 'agraph'"); }
	}
	
	public static void runAgraph() throws Exception {		
		// load ground truth
		HashSet<String> groundTruth = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader("./resource/LUBM45groundTruth.txt"));
		String line;
		while((line = br.readLine()) != null) { groundTruth.add(line); }
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
		String backgroundOntologyPath = "./resource/univ-bench.owl";
		
		for(int i = 0; i < numberOfTriples_.length; ++i) {
			AGCache agfefo = new AGCache(numberOfTriples_[i], evictTimes, groundTruth, Launcher.agraphServerURL, Launcher.agraphUsername, Launcher.agraphPassword, Launcher.agraphCatalog, Launcher.agraphRepository);
			
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
	
	public static void runStardog() throws Exception {
		// load ground truth
		HashSet<String> groundTruth = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader("./resource/LUBM45groundTruth.txt"));
		String line;
		while((line = br.readLine()) != null) {
			groundTruth.add(line);
		}
		br.close();
		System.out.println("[INFO] ground truth loaded");		
		
		// The following cases are for benchmark
		// Map<cacheSize, evictionAmount>
		Map<Integer, ArrayList<Integer>> cases = new LinkedHashMap<Integer, ArrayList<Integer>> ();
		cases.put(10, new ArrayList<Integer>(Arrays.asList(2,5,7,10)));
		cases.put(100, new ArrayList<Integer>(Arrays.asList(25,50,75,100))); 
		cases.put(1000, new ArrayList<Integer>(Arrays.asList(250,500,750,1000)));

		int[] numberOfTriples_ = {1,10,100};
		
		int evictTimes = 10; 
		String backgroundOntologyPath = "./resource/univ-bench.owl";
		String disk = "disk";
		String memory = "mem";

		// number of triples in a graph: 1, 10, 100, 1000
		for(int i = 0; i < numberOfTriples_.length; ++i) {
			if(numberOfTriples_[i] == 10) {
				evictTimes = 5;
			} else if ( numberOfTriples_[i] == 100) {
				evictTimes = 3;
			}
			StardogCache sdfefomem = new StardogCache(numberOfTriples_[i], evictTimes, groundTruth, "FEFO",  memory, Launcher.stardogServerURL, Launcher.stardogUsername, Launcher.stardogPassword);
			StardogCache sdlfumem = new StardogCache(numberOfTriples_[i],	evictTimes, groundTruth, "LFU", memory, Launcher.stardogServerURL, Launcher.stardogUsername, Launcher.stardogPassword);
			StardogCache sdlrumem = new StardogCache(numberOfTriples_[i], evictTimes, groundTruth, "LRU", memory, Launcher.stardogServerURL, Launcher.stardogUsername, Launcher.stardogPassword);

			StardogCache sdfefodisk = new StardogCache(numberOfTriples_[i], evictTimes, groundTruth, "FEFO", disk, Launcher.stardogServerURL, Launcher.stardogUsername, Launcher.stardogPassword);
			StardogCache sdlfudisk = new StardogCache(numberOfTriples_[i], evictTimes, groundTruth, "LFU", disk, Launcher.stardogServerURL, Launcher.stardogUsername, Launcher.stardogPassword);
			StardogCache sdlrudisk = new StardogCache(numberOfTriples_[i], evictTimes, groundTruth, "LRU", disk, Launcher.stardogServerURL, Launcher.stardogUsername, Launcher.stardogPassword);

			for(Map.Entry<Integer, ArrayList<Integer>> entry :
				cases.entrySet()) {
				sdfefomem.setSize(entry.getKey());
				sdlfumem.setSize(entry.getKey());
				sdlrumem.setSize(entry.getKey());

				sdfefodisk.setSize(entry.getKey());
        		sdlfudisk.setSize(entry.getKey());
				sdlrudisk.setSize(entry.getKey());

				ArrayList<Integer> evictArray = entry.getValue();
				for(int j : evictArray){
					sdfefomem.setEvictAmount(j);
					sdlfumem.setEvictAmount(j);
					sdlrumem.setEvictAmount(j);

					sdfefomem.run(backgroundOntologyPath);
					sdlfumem.run(backgroundOntologyPath);
					sdlrumem.run(backgroundOntologyPath);

					sdfefodisk.setEvictAmount(j);
					sdlfudisk.setEvictAmount(j);
					sdlrudisk.setEvictAmount(j);

					sdfefodisk.run(backgroundOntologyPath);
					sdlfudisk.run(backgroundOntologyPath);
					sdlrudisk.run(backgroundOntologyPath);
				}
			}
			sdfefomem.clean();
			sdlfumem.clean();
			sdlrumem.clean();

			sdfefodisk.clean();
			sdlfudisk.clean();
			sdlrudisk.clean();
		}		
	}
}
