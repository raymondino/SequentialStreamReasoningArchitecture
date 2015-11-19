/*
 * @Author Rui Yan
 * FIFO class is first-in-first-out cache eviction policy
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
import java.util.ArrayList;
import java.util.LinkedList;

import org.openrdf.model.Model;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.UnsupportedQueryResultFormatException;
import org.openrdf.rio.RDFFormat;

import com.complexible.common.openrdf.model.Models2;
import com.complexible.common.rdf.model.Values;
import com.complexible.stardog.StardogException;
import com.pnnl.shyre.stardog.client.SnarlClient;

public class StardogFIFO {	
	private int size; // cache size
	private int graphID; // a incremental value to label different graphs
	private int evictAmount; // how many graph ids are evicted at one time when the cache is full
	private int numberOfTriples; // the number of the triples that are in the same graph
	private LinkedList<String> cacheContentOfGraphIds; // linked list works as a queue to realize FIFO eviction
	private ArrayList<String> groundTruth;
	
	private String serverURL;
	private String username;
	private String password;
	private SnarlClient client; // the client that talks to the backend Stardog triplestore 
	
	private PrintWriter metricRecorder; // to record the metrics
	private BufferedReader br; // to read data from file for stream simulation
	
	// FIFO class contructor
	public StardogFIFO(int numberOfTriples_, String serverURL_, String username_, String password_) 
			throws IOException {
		this.numberOfTriples = numberOfTriples_;
		this.serverURL = serverURL_;
		this.username = username_;
		this.password = password_;
		this.cacheContentOfGraphIds = new LinkedList<String>();
		this.client = new SnarlClient(serverURL, "stardogfifo", username, password);

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
	
	// FIFO run
	public void run(String backgroundOntologyPath) 
			throws IOException, TupleQueryResultHandlerException, QueryEvaluationException, UnsupportedQueryResultFormatException {
		System.out.println("[INFO] Stardog FIFO cache starts: cache size = " + this.size);
		System.out.println("                          evict amount = " + this.evictAmount);
		System.out.println("                          server url = " + this.serverURL);
		
		String benchmarkFilePath = "files/stardogfifo/sdfifo_bench_"+ size +"_"+this.evictAmount+".csv";
		this.metricRecorder = new PrintWriter(benchmarkFilePath, "UTF-8");
		this.metricRecorder.println("sparql, precision, recall, fmeasure, evict, memory");
		
		this.loadBkOnto(this.client, backgroundOntologyPath);
		
		this.loadCacheToFull();
		System.out.println("[INFO] Cache is full, now rocking & roll");
		
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
			flag = this.streamEmulation(); // if the end of the file, system stops
			this.metricRecorder.flush();
		}
		this.br.close();
		System.out.println("[INFO] Stardog FIFO cache finished");
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
		this.streamEmulation();
	}
	
	// simulate the streaming data by reading from disk line by line
	public boolean streamEmulation() 
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
				else {return false;}
			}
			this.cacheContentOfGraphIds.add("http://example.org/graphID"+(this.graphID++));
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
	}
		
	// Evict evictAmount graphs using SPARQL v1.1 update drop argument
	public void evict(){
//		System.out.println("[INFO] Stardog FIFO Evicting, cache size = " + this.size + " evict amount = " + this.evictAmount); // for display only	
//		for(String x : this.cacheContentOfGraphIds) {
//			System.out.println(x);
//		}
		String queryString = "";
		for(int i = 0; i < this.evictAmount; i++){
			String graph = this.cacheContentOfGraphIds.remove();
			queryString = queryString + "drop graph <" + graph + ">;";
		}		
		System.out.println(queryString);// for display only
		this.client.getAConn().update(queryString.substring(0, queryString.length() - 1)).execute();
	}
	
	// close all the connections and release all the resources
	public void clean(){
		this.client.cleanUp();
	}
}
