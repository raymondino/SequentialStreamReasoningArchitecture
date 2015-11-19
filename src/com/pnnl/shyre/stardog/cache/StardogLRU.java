/*
 * @Author Rui Yan
 * LRU class is least-recently-used cache eviction policy
 */

package com.pnnl.shyre.stardog.cache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.time.LocalTime;
import java.util.ArrayList;
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

public class StardogLRU {
	private int size; // cache size
	private int graphID; // a incremental value to label different graphs
	private int evictAmount; // how many graph ids are evicted at one time when the cache is full
	private int numberOfTriples; // the number of the triples that are in the same graph
	private PriorityQueue<GraphIdCounterPair> cacheContentOfGraphIds; // linked list works as a queue to realize LRU eviction
	private ArrayList<String> groundTruth;
	
	private String serverURL;
	private String username;
	private String password;
	private SnarlClient client; // the client that talks to the backend Stardog triplestore 
	
	private PrintWriter metricRecorder; // to record the metrics
	private BufferedReader br; // to read data from file for stream simulation

	// LRU class contructor
	public StardogLRU(int numberOfTriples_, String serverURL_, String username_, String password_) 
			throws IOException {
		this.numberOfTriples = numberOfTriples_;
		this.serverURL = serverURL_;
		this.username = username_;
		this.password = password_;
		this.cacheContentOfGraphIds = new PriorityQueue<GraphIdCounterPair>();
		this.client = new SnarlClient(serverURL, "stardoglru", username, password);
		
		// load groud truth for fmeasure bench
		this.groundTruth = new ArrayList<String> ();
		this.br = new BufferedReader(new FileReader("files/groundTruth.txt"));
		String line;
		while((line = br.readLine()) != null) {
			this.groundTruth.add(line);
		}
		this.br.close();
		System.out.println("[INFO] ground truth loaded");
	}
	
	// set cache size
	public void setSize(int value) {
		this.size = value;
	}
	
	// set evict amount
	public void setEvictAmount(int amount) {
		this.evictAmount = amount;
	}
	
	// LRU run
	public void run(String backgroundOntologyPath)
			throws IOException, TupleQueryResultHandlerException, QueryEvaluationException, UnsupportedQueryResultFormatException {
		System.out.println("[INFO] Stardog LRU cache starts: cache size = " + this.size);
		System.out.println("                         evict amount = " + this.evictAmount);
		System.out.println("                         server url = " + this.serverURL);
		
		String benchmarkFilePath = "files/stardoglru/sdlru_bench_"+ size +"_"+this.evictAmount+".csv";
		this.metricRecorder = new PrintWriter(benchmarkFilePath, "UTF-8");
		this.metricRecorder.println("sparql, precision, recall, fmeasure, evict, memory");
		
		this.loadBkOnto(this.client, backgroundOntologyPath);
		
		this.loadCacheToFull();
		
		boolean flag = true;
		while(flag) {			
			// sparql
			this.sparql();
			
			// cache eviction
			long evictStartTime = System.currentTimeMillis();
			this.evict();
			long evictEndTime = System.currentTimeMillis();
			this.metricRecorder.print((evictEndTime - evictStartTime) + ", ");
			this.metricRecorder.println((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/(1024*1024));
		
			// add data after eviction
			flag = this.StreamEmulation(); // if the end of the file, system stops
			this.metricRecorder.flush();
		}
		this.br.close();
		System.out.println("[INFO] Stardog LRU cache finished");
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
		this.br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("files/LUBMSmallFull.nt"))));
		this.StreamEmulation();
	}
	
	// simulate the streaming data by reading from disk line by line
	public boolean StreamEmulation() 
			throws IOException {
		String line = null;
		while(this.cacheContentOfGraphIds.size() < this.size){ // that means you can read and feed the data
			for(int i = 0; i < this.numberOfTriples; ++i){
				if ( (line = this.br.readLine()) != null ){
					line = line.replaceAll("<", "");
					line = line.replaceAll(">", "");
					String[] parts = line.split(" ");
					String s = parts[0];
					String p = parts[1];
					if (parts[2].contains("http")) {
						String o = parts[2].substring(0, parts[2].length());
						Model m = Models2.newModel(Values.statement(Values.iri(s), Values.iri(p), Values.iri(o)));
						this.client.addModel(m, "http://example.org/graphID"+this.graphID);
					}
					else {
						String o = parts[2].substring(1, parts[2].length()-1);
						Model m = Models2.newModel(Values.statement(Values.iri(s), Values.iri(p), Values.literal(o)));
						this.client.addModel(m, "http://example.org/graphID"+this.graphID);
					}
				}
				else {
					return false;
				}
			}

			this.cacheContentOfGraphIds.offer(new GraphIdCounterPair(("http://example.org/graphID"+(this.graphID++)), -1, LocalTime.now()));
		}
		return true;
	}
	
	// SPARQL execution, reasoning happens at the query time
	public void sparql() 
			throws TupleQueryResultHandlerException, QueryEvaluationException, UnsupportedQueryResultFormatException, IOException{
		//String queryString = "select distinct ?s where { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#Chair>}";
		String queryString = "select distinct ?s where "
				+ "{ ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
				+ "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#Professor>. }";

		long sparqlStartTime = System.currentTimeMillis();
		TupleQueryResult resultSet = this.client.getAConn().select(queryString).execute();
		long sparqlEndTime = System.currentTimeMillis();
		this.metricRecorder.print((sparqlEndTime - sparqlStartTime)+", ");

		ArrayList<String> results = new ArrayList<String> ();
		while(resultSet.hasNext()) {
			results.add(resultSet.next().getValue("s").toString());
		}
		resultSet.close();
		this.fMeasureBench(results);
		this.explain(queryString);
	}

	// calculate the precision, recall & f-measure
	private void fMeasureBench(ArrayList<String> results) {
		if(results.size() == 0) {
			this.metricRecorder.print("0, 0, 0, ");
		}
		else {
			int counter = 0;
			for(String x : results) {
				if(results.contains(x)) {
					counter++;
				}
			}
			double precision = (double) counter / results.size();
			double recall = (double) counter / this.groundTruth.size();
			this.metricRecorder.print(precision + ", " + recall + ", " + 2*precision*recall/(precision + recall) + ", " );
		}
		this.metricRecorder.flush();
	}
	
	// Evict evictAmount graphs using SPARQL v1.1 update drop argument
	public void evict(){
//		System.out.println("[INFO] Stardog LRU Evicting: cache size = " + this.size + " evict amount = " + this.evictAmount);
//		for(GraphIdCounterPair x: this.cacheContentOfGraphIds) {
//			System.out.println(x.graphId + " " + x.time);
//		}
		String queryString = "";
		for(int i = 0; i < this.evictAmount; i++){
			GraphIdCounterPair graph = this.cacheContentOfGraphIds.poll();
			queryString = queryString + "drop graph <" + graph.graphId + ">;"; // need to test the format of the graphID
		}
//		System.out.println("query string: " + queryString);// for display only
		this.client.getAConn().update(queryString.substring(0, queryString.length() - 1)).execute();
	}
	
	// Explain the query so that to decide the ranking of graph IDs to decide which ones to be evicted
	private void explain(String queryString) 
			throws TupleQueryResultHandlerException, QueryEvaluationException, UnsupportedQueryResultFormatException, IOException{
		TupleQueryResult aResult = this.client
				                             .getAConn()
                                             .select(queryString)
                                             .execute();
		ArrayList<Set<IRI>> graphIds = new ArrayList<Set<IRI>>();
		while(aResult.hasNext()){
			Value s = aResult.next().getValue("s");
			Iterable<Proof> proofs = this.client.getAConn()
					.explain(Values.statement(Values.iri(s.toString()), 
					Values.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
					Values.iri("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person")))
					.computeNamedGraphs()
					.proofs();
			Iterator<Proof> proof_itr = proofs.iterator();
			while(proof_itr.hasNext()) {
				Proof aProof = proof_itr.next();
				getGraphIDs(aProof, graphIds);
			}
		}
		for(int i = 0; i < graphIds.size(); ++i){
			for(IRI id: graphIds.get(i)) {
				if(this.cacheContentOfGraphIds.contains(id.toString()));
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
		// dump graphids from graphIds arraylist, save them with their counters.
		ArrayList<GraphIdCounterPair> lists = new ArrayList<GraphIdCounterPair>();
		int index = -1;
		for(Set<IRI> aSet: graphIds) {
			for(IRI aIRI: aSet) {
				index = contains(lists, aIRI.toString());
				if(index >= 0) {
					GraphIdCounterPair x = lists.get(index);
					lists.set(index, new GraphIdCounterPair(x.graphId, -1, LocalTime.now()));
				}
				else {
					lists.add(new GraphIdCounterPair(aIRI.toString(), -1, LocalTime.now()));
				}
			}
		}
		// remove the old pair
		Iterator<GraphIdCounterPair> itr = this.cacheContentOfGraphIds.iterator();
		ArrayList<GraphIdCounterPair> toRemove = new ArrayList<GraphIdCounterPair>();
		while(itr.hasNext()) {
			GraphIdCounterPair oldPair = itr.next();
			index = contains(lists, oldPair.graphId);
			if(index >= 0) {
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
	
	// a helpfer function
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
