/*
 * @Author Rui Yan
 * LFU class is least-frequently-used cache eviction policy
 */

package com.pnnl.shyre.stardog.cache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;

import org.openrdf.model.IRI;
import org.openrdf.model.Model;
import org.openrdf.model.Value;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.UnsupportedQueryResultFormatException;
import org.openrdf.rio.RDFFormat;

import com.complexible.common.openrdf.model.Models2;
import com.complexible.common.rdf.model.Values;
import com.complexible.stardog.StardogException;
import com.complexible.stardog.reasoning.Proof;
import com.complexible.stardog.reasoning.ProofType;
import com.pnnl.shyre.stardog.client.SnarlClient;
import com.pnnl.shyre.utilities.GraphIdCounterPair;

public class StardogLFU {
	
	// cache size
	private int size; 
	// a incremental value to label different graphs
	private int graphID; 
	// how many graph IDs are evicted at one time when the cache is full
	private int evictAmount; 
	// increment by 1 after one eviction
	private int evictCounter; 	
	// total eviction times
	private int totalEvictTimes; 
	// query string for SPARQL drop
	private String dropQueryString;
	// delete data counter
	private int toDeleteCounter;
	// the number of the triples that are in the same graph
	private int numberOfTriples; 
	// linked list works as a queue to realize FIFO eviction
	private PriorityQueue<GraphIdCounterPair> cacheContentOfGraphIds; 
	// ground truth for f-measure
	private HashSet<String> groundTruth;
	
	private String cacheType;
	private String serverURL;
	private String username;
	private String password;
	// the client that talks to the back-end Stardog triplestore
	private SnarlClient client;  
	
	// to record the metrics
	private PrintWriter metricRecorder; 
	// to read data from file for stream simulation
	private BufferedReader br; 

	// LFU class constructor
	public StardogLFU(int numberOfTriples_, int totalEvictTimes_, 
			HashSet<String> groundTruth_, String cacheType_,
			String serverURL_, String username_, String password_) 
			throws IOException {
		
		this.evictAmount = 0;
		this.evictCounter = 1;
		this.totalEvictTimes = totalEvictTimes_;
		this.numberOfTriples = numberOfTriples_;
		this.toDeleteCounter = 0;
		this.dropQueryString = "";
		this.groundTruth = groundTruth_;
		
		this.cacheType = cacheType_;
		this.serverURL = serverURL_;
		this.username = username_;
		this.password = password_;
		
		this.cacheContentOfGraphIds = new PriorityQueue<GraphIdCounterPair>();
		this.client = new SnarlClient(serverURL, cacheType, 
				"stardoglfu"+cacheType_, username, password);
	}
	
	// set cache size
	public void setSize(int value) {
		this.size = value;
	}
	
	// set evict amount
	public void setEvictAmount(int amount) {
		this.evictAmount = amount;
	}
	
	// LFU run
	public void run(String backgroundOntologyPath)
			throws IOException, TupleQueryResultHandlerException, 
			QueryEvaluationException, UnsupportedQueryResultFormatException {

		// clear everything for a new run
		this.cacheContentOfGraphIds.clear();
		this.client.emptyDB();
		System.out.println("**********************************");
		System.out.println("[INFO] Stardog LFU cache starts: "
				+ "cache size = " + this.size);
		System.out.println("                                  "
				+ "evict amount = " + this.evictAmount);
		
		// write benchmark files
		String benchmarkFilePath = "files/stardoglfu" + this.cacheType 
				+ "/sdlfu" + this.cacheType + "_bench_"+this.numberOfTriples 
				+ "_" + this.size +"_"+this.evictAmount+".csv";
		this.metricRecorder = new PrintWriter(benchmarkFilePath, "UTF-8");
		this.metricRecorder.println("sparql, precision, recall, fmeasure, "
				+ "explain, evict, memory");
		
		this.loadBkOnto(this.client, backgroundOntologyPath);
		
		this.loadCacheToFull();
		
		boolean noEndFile = true;
		boolean evicting = true;
		while(noEndFile && evicting) {	
			
			// SPARQL
			this.sparql();
			
			// cache eviction
			long evictStartTime = System.currentTimeMillis();
			evicting = this.evict();
			long evictEndTime = System.currentTimeMillis();
			this.metricRecorder.print((evictEndTime - evictStartTime) + ", ");
			this.metricRecorder.println((Runtime.getRuntime().totalMemory() - 
					Runtime.getRuntime().freeMemory())/(1024*1024));
		
			// add data after eviction
			noEndFile = this.streamEmulation(); 
			this.metricRecorder.flush();
		}	
		this.br.close();
		System.out.println("==================================");
	}
	
	// load background ontology
	public void loadBkOnto(SnarlClient client, String backgroundOntologyPath) 
			throws StardogException, FileNotFoundException {
		System.out.println("[INFO] loading background ontology");
		client.getAConn().begin();
		client.getAConn()
		      .add()
		      .io()
		      .format(RDFFormat.RDFXML)
		      .stream(new FileInputStream(backgroundOntologyPath));
		client.getAConn().commit();
		System.out.println("[INFO] background ontology loaded");
	}
	
	// load the cache to its capacity
	public void loadCacheToFull() 
			throws IOException {
		
		this.graphID = 0;
		this.br = new BufferedReader(
				   new InputStreamReader(
				    new FileInputStream(
				     new File("files/LUBM45.nt"))));
		this.streamEmulation();
	}
	
	// simulate the streaming data by reading from disk line by line
	public boolean streamEmulation() 
			throws IOException {
		String line = null;
		while(this.cacheContentOfGraphIds.size() < this.size){
			System.out.println("[INFO] current cache load:" + 
		                       this.cacheContentOfGraphIds.size());
			for(int i = 0; i < this.numberOfTriples; ++i){
				if ( (line = this.br.readLine()) != null ){
					line = line.replaceAll("<", "");
					line = line.replaceAll(">", "");
					String[] parts = line.split(" ");
					String s = parts[0];
					String p = parts[1];
					if (parts[2].contains("http")) {
						String o = parts[2].substring(0, parts[2].length());
						Model m = Models2.newModel(Values.statement(
								Values.iri(s), 
								Values.iri(p), 
								Values.iri(o)));
						this.client.addModel(m, "http://example.org/graphID"
								+ this.graphID);
					}
					else {
						String o = parts[2].substring(1, parts[2].length()-1);
						Model m = Models2.newModel(Values.statement(
								Values.iri(s), 
								Values.iri(p), 
								Values.literal(o)));
						this.client.addModel(m, "http://example.org/graphID"
								+this.graphID);
					}
				}
				else {return false;}
			}
			this.cacheContentOfGraphIds.add(new GraphIdCounterPair(
					"http://example.org/graphID"+(this.graphID++), 
					0, 
					LocalTime.now(), 
					"LFU"));
		}
		return true;
	}
	
	// SPARQL execution, reasoning happens at the query time
	public void sparql() 
			throws TupleQueryResultHandlerException, QueryEvaluationException, 
			UnsupportedQueryResultFormatException, IOException{
		// find all expired data to avoid them participating the query:
		this.toDeleteCounter = 0;
		this.dropQueryString = "";
		ArrayList<GraphIdCounterPair> expiredData = 
				new ArrayList<GraphIdCounterPair> ();
		LocalTime evictionTime = LocalTime.now();
		for(GraphIdCounterPair x: this.cacheContentOfGraphIds) {
			System.out.print(this.size + ", " + this.evictAmount + ", LFU, " + 
		                     x.graphId + ", " + x.count + ", " + 
					         x.expirationTime);
			if(x.expirationTime.isBefore(evictionTime)) {
				expiredData.add(x);
				dropQueryString += "drop graph <" + x.graphId + ">;";
				System.out.println(", expired");
				++toDeleteCounter;
			} else {
				System.out.println();
			}
		}
		if(!expiredData.isEmpty()) {
			// delete expired data from the cache
			for(GraphIdCounterPair x: expiredData) {
				this.cacheContentOfGraphIds.remove(x);
			}
			// delete the expired data from the database
			this.client.getAConn()
			.update(this.dropQueryString
					.substring(0, this.dropQueryString.length() - 1)).execute();	
		}		
	
		// after deleting expired data, load the cache again
		this.streamEmulation();		
		
		// String queryString = "select distinct ?s where 
		// { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> 
		// <http://swat.cse.lehigh.edu/onto/univ-bench.owl#Chair>}";
		String queryString = "select distinct ?s where "
				+ "{ ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
				+ "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#Professor>."
				+ "}";
	
		long sparqlStartTime = System.currentTimeMillis();
		TupleQueryResult resultSet = this.client
				.getAConn().select(queryString).execute();
		long sparqlEndTime = System.currentTimeMillis();
		this.metricRecorder.print((sparqlEndTime - sparqlStartTime)+", ");

		ArrayList<String> results = new ArrayList<String> ();
		while(resultSet.hasNext()) {
			results.add(resultSet.next().getValue("s").toString());
		}
		resultSet.close();
		
		this.fMeasureBench(results);
		
		if(this.size != this.evictAmount) {
			long explainStartTime = System.currentTimeMillis();
			this.explain(queryString);
			long explainEndTime = System.currentTimeMillis();			
			this.metricRecorder.print((explainEndTime - explainStartTime) 
					+", ");	
		} else {
			this.metricRecorder.print("0, ");
		}			
	}
	
	// calculate the precision, recall & f-measure
	private void fMeasureBench(ArrayList<String> results) {
		if(results.size() == 0) {
			this.metricRecorder.print("0, 0, 0, ");
		}
		else {
			int counter = 0;
			for(String x : results) {
				if(groundTruth.contains(x)) {
					counter++;
				}
			}
			double precision = (double) counter / results.size();
			double recall = (double) counter / this.groundTruth.size();
			this.metricRecorder.print(precision + ", " + recall + ", " 
			+ 2*precision*recall/(precision + recall) + ", " );
		}
	}
	
	// Evict evictAmount graphs using SPARQL v1.1 update drop argument
	public boolean evict(){
		
		this.dropQueryString="";
		
		// limit the eviction times so as to save time during benchmark
		if(this.evictCounter > this.totalEvictTimes) {
			this.evictCounter = 1;
			return false;
		} else { 
			this.evictCounter++;
		}
		// if evict the whole cache each time
		if(this.evictAmount == this.size) {
			while(this.cacheContentOfGraphIds.size() != 0) {
				GraphIdCounterPair x = this.cacheContentOfGraphIds.poll();
				this.dropQueryString += "drop graph <" + x.graphId + ">;";
			}
		}
		// when evictAmount < size
		else {
			// check if the amount of the expired data exceeds the evictAmount
			while(this.toDeleteCounter < this.evictAmount) {
				GraphIdCounterPair x = this.cacheContentOfGraphIds.poll();
				this.dropQueryString += "drop graph <" + x.graphId + ">;";
				++toDeleteCounter;
			}			
		}
		// if this.toDeleteCounter >= this.evictAmount, dropQueryString will be 
		// empty. 
		if(!this.dropQueryString.equals("")) {
			// delete the data from the database
			this.client.getAConn()
			.update(this.dropQueryString
					.substring(0, this.dropQueryString.length() - 1)).execute();	
		}		
		return true;
	}
	
	// Explain the query so that to decide the ranking of graph IDs 
	// to decide which ones to be evicted
	public void explain(String queryString) 
			throws TupleQueryResultHandlerException, QueryEvaluationException, 
			UnsupportedQueryResultFormatException, IOException{
		TupleQueryResult aResult = this.client
				.getAConn().select(queryString).execute();
		ArrayList<Set<IRI>> graphIds = new ArrayList<Set<IRI>>();
		while(aResult.hasNext()){
			Value s = aResult.next().getValue("s");
			Iterable<Proof> proofs = this.client.getAConn()
					.explain(Values.statement(Values.iri(s.toString()), 
					Values.iri
					("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
					Values.iri
					("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person")))
					.computeNamedGraphs()
					.proofs();
			Iterator<Proof> proof_itr = proofs.iterator();
			while(proof_itr.hasNext()) {
				Proof aProof = proof_itr.next();
				getGraphIDs(aProof, graphIds);
			}
		}
		this.update(graphIds);
	}
	
	private void getGraphIDs(Proof aProof, ArrayList<Set<IRI>> graphIDs) {
		 if(!aProof.hasChildren() 
				 && aProof.getType() == ProofType.ASSERTED
				 && !aProof.getNamedGraphs().isEmpty()) {
			 graphIDs.add(aProof.getNamedGraphs());
			 return;
		 }
		 if(aProof.getType() == ProofType.INFERRED) {
			 Iterator<Proof> proof_itr = aProof.getChildren().iterator();
			 while(proof_itr.hasNext()) {
				 getGraphIDs(proof_itr.next(), graphIDs);
			 }
		 }
		 return;
	}
	
	private void update(ArrayList<Set<IRI>> graphIds){
		// dump graph IDs from graphIds ArrayList, 
		// save them with their counters.
		ArrayList<GraphIdCounterPair> lists = 
				new ArrayList<GraphIdCounterPair>();
		int index = -1;
		for(Set<IRI> aSet: graphIds) {
			for(IRI aIRI: aSet) {
				index = contains(lists, aIRI.toString());
				if(index >= 0) {
					GraphIdCounterPair x = lists.get(index);
					lists.set(index, new GraphIdCounterPair(x.graphId, 
							++(x.count), null, null, "LFU"));
				}
				else {
					lists.add(new GraphIdCounterPair(aIRI.toString(), 
							1,  null, null, "LFU"));
				}
			}
		}
		
		// need to get the common pairs that are both in the cache and update 
		// list. also need to get the common pairs' counts in that cache, 
		// add it to the new count, then remove the old pair
		Iterator<GraphIdCounterPair> itr = 
				this.cacheContentOfGraphIds.iterator();
		ArrayList<GraphIdCounterPair> toRemove = 
				new ArrayList<GraphIdCounterPair>();
		while(itr.hasNext()) {
			GraphIdCounterPair oldPair = itr.next();
			index = contains(lists, oldPair.graphId);
			if(index >= 0) {
				GraphIdCounterPair x = lists.get(index);
				lists.set(index, new GraphIdCounterPair(x.graphId, 
						(x.count + oldPair.count), 
						oldPair.arrivalTime, oldPair.expirationTime, "LFU"));
				toRemove.add(oldPair);
			}
		}
		
		// remove all the pre-update elements
		for(GraphIdCounterPair x: toRemove) {
			this.cacheContentOfGraphIds.remove(x);
		}
		
		// add all the updated pairs
		for(GraphIdCounterPair newPair : lists) {
			this.cacheContentOfGraphIds.offer(newPair);
		}		
	}
	
	// a helper function
	private Integer contains(ArrayList<GraphIdCounterPair> lists, String aIRI) {
		if(lists.size() == 0) {
			return -1;
		}
		for(GraphIdCounterPair alist: lists) {
			if(alist.graphId.equals(aIRI)) {
				return lists.indexOf(alist);
			}
		}
		return -1;
	}
	
	// close all the connections and release all the resources
	public void clean(){
		this.client.cleanUp();
	}

}
