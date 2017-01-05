package launcher;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.LinkedHashMap;

import stardog.Cache;


public class Launcher {
	private static String serverURL = "http://localhost:5820";
	private static String username = "admin";
	private static String password = "admin";
		
	public static void main(String[] args) throws Exception {
		if(args.length !=  1) {
			System.out.println("wrong input argument, please specify either 'stardog' or 'agraph'");
		}
		if(args[0].equals("stardog")) { runStardog(); }
		else { runAgraph(); }
	}
	
	public static void runAgraph() throws Exception {
		
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
			Cache sdfefomem = new Cache(numberOfTriples_[i],
					evictTimes, groundTruth, "FEFO",  memory,
					Launcher.serverURL, Launcher.username, Launcher.password);
			Cache sdlfumem = new Cache(numberOfTriples_[i],
					evictTimes, groundTruth, "LFU", memory,
					Launcher.serverURL, Launcher.username, Launcher.password);
			Cache sdlrumem = new Cache(numberOfTriples_[i],
					evictTimes, groundTruth, "LRU", memory,
					Launcher.serverURL, Launcher.username, Launcher.password);

			Cache sdfefodisk = new Cache(numberOfTriples_[i],
					evictTimes, groundTruth, "FEFO", disk,
					Launcher.serverURL, Launcher.username, Launcher.password);
			Cache sdlfudisk = new Cache(numberOfTriples_[i],
			evictTimes, groundTruth, "LFU", disk,
					Launcher.serverURL, Launcher.username, Launcher.password);
			Cache sdlrudisk = new Cache(numberOfTriples_[i],
			evictTimes, groundTruth, "LRU", disk,
					Launcher.serverURL, Launcher.username, Launcher.password);

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
