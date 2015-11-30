package com.pnnl.shyre.allegrograph.cache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.openrdf.OpenRDFException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFParseException;

import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.franz.agraph.jena.AGGraph;
import com.franz.agraph.jena.AGGraphMaker;
import com.franz.agraph.jena.AGInfModel;
import com.franz.agraph.jena.AGModel;
import com.franz.agraph.jena.AGQuery;
import com.franz.agraph.jena.AGQueryExecutionFactory;
import com.franz.agraph.jena.AGQueryFactory;
import com.franz.agraph.jena.AGReasoner;
import com.franz.agraph.repository.AGRepositoryConnection;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;
import com.pnnl.shyre.allegrograph.client.AGClient;

public class AGFIFO {

	private static String serverURL = "http://localhost:10035";
	private static String catalog = "system";
	private static String repository = "agfifo";
	private static String username = "rui";
	private static String password = "yr";
	
	private int evictTime;
	private int evictTimes;
	private int size; // cache size
	private int graphID; // a incremental value to label different graphs
	private int evictAmount; // how many graph ids are evicted at one time when the cache is full
	private int numberOfTriples; // the number of the triples that are in the same graph
	private LinkedList<String> cacheContentOfGraphIds; // linked list works as a queue to realize FIFO eviction
	private ArrayList<String> groundTruth;
	
	private AGClient client;
	private AGRepositoryConnection con;
	private AGModel model;
	private AGInfModel infModel;
	private AGGraph modelGraph;
	private AGGraphMaker graphMaker;
	private AGReasoner reasoner;
	private BufferedReader br;
	
	private PrintWriter metricRecorder;
		
	public AGFIFO(int numberOfTriples_, int evictTimes_) 
			throws Exception{
		this.evictTime = 1;
		this.evictTimes = evictTimes_;
		this.numberOfTriples = numberOfTriples_;
		this.cacheContentOfGraphIds = new LinkedList<String>();
		this.client = new AGClient(AGFIFO.serverURL, AGFIFO.catalog, AGFIFO.repository, 
				AGFIFO.username, AGFIFO.password);
		this.client.connect();
		this.client.initTS();
				
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
	
	// AGFIFO run
	public void run(String backgroundOntologyPath) 
			throws IOException, OpenRDFException {
		this.cacheContentOfGraphIds.clear();
		this.client.emptyDB(); // need to be examined
		System.out.println("[INFO] Allegrograph FIFO cache starts: cache size = " + this.size);
		System.out.println("                          evict amount = " + this.evictAmount);
		System.out.println("                          server url = " + AGFIFO.serverURL);
		
		String benchmarkFilePath = "files/agfifo/agfifo_bench_"+ this.numberOfTriples + "_" + size +"_"+this.evictAmount+".csv";
		this.metricRecorder = new PrintWriter(benchmarkFilePath, "UTF-8");
		this.metricRecorder.println("sparql, precision, recall, fmeasure, evict, memory");
		
		this.loadBkOnto(this.client, backgroundOntologyPath);
		
		this.loadCacheToFull();
		
		boolean flag = true;
		boolean flag1 = true;
		while(flag && flag1) {			
			// sparql
			this.sparql();
			
			// cache eviction
			long evictStartTime = System.currentTimeMillis();
			flag1 = this.evict();
			long evictEndTime = System.currentTimeMillis();
			this.metricRecorder.print((evictEndTime - evictStartTime) + ", ");
			this.metricRecorder.println((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/(1024*1024));
			
			// add data after eviction
			flag = this.streamEmulation(); // if the end of the file, system stops
			this.metricRecorder.flush();
		}
		this.infModel.close();
		this.model.close();
		this.metricRecorder.close();
		this.br.close();
		System.out.println("[INFO] AGFIFO cache finished");
	}
	
	// load background ontology
	public void loadBkOnto(AGClient client, String backgroundOntologyPath) 
			throws RDFParseException, RepositoryException, IOException {
		System.out.println("[INFO] loading background ontology");
		this.con = this.client.getAGConn(); 
		this.graphMaker = new AGGraphMaker(this.con);
		this.model = new AGModel(this.graphMaker.createGraph("http://fifocacheeviction.org/bkgdOnto"));
		this.model.read(new FileInputStream(backgroundOntologyPath), "");
		this.modelGraph = this.model.getGraph();
		this.reasoner = new AGReasoner("restriction");
		closeBeforeExit(this.con);		
		System.out.println("[INFO] background ontology loaded");
	}
	
	// load the cache to its capacity
	public void loadCacheToFull() 
			throws IOException {
		this.graphID = 0;
		this.br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("files/LUBMSmallFull.nt"))));
		this.streamEmulation();
	}
	
	// simualte the streaming data 
	public boolean streamEmulation() 
			throws IOException{
		String line = null;
		while(this.cacheContentOfGraphIds.size() < this.size){ // that means you can read and feed the data
			AGGraph graph = this.graphMaker.createGraph("http://fifocacheeviction.org/graph"+this.graphID);
			for(int i = 0; i < this.numberOfTriples; ++i){
				if ( (line = this.br.readLine()) != null ){
					line = line.replaceAll("<", "");
					line = line.replaceAll(">", "");
					String[] parts = line.split(" ");
					Node s = NodeFactory.createURI(parts[0]);
					Node p = NodeFactory.createURI(parts[1]);
					if (parts[2].contains("http")) {
						Node o = NodeFactory.createURI(parts[2].substring(0, parts[2].length()));
						graph.add(new Triple(s, p, o));
					}
					else {
						Node o = NodeFactory.createLiteral(parts[2].substring(1, parts[2].length()-1));
						graph.add(new Triple(s, p, o));
					}
				}
				else 
					return false;
			}
			this.cacheContentOfGraphIds.add("http://fifocacheeviction.org/graph"+(this.graphID++));
			this.modelGraph = this.graphMaker.createUnion(this.modelGraph,graph);
		}
		return true;
	}

	// sparql
	public void sparql(){
		this.infModel = new AGInfModel(this.reasoner, this.model);
		String queryString = "select distinct ?s "
				+ "where { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
				+ "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#Professor>. } " ;
		AGQuery sparql = AGQueryFactory.create(queryString);
		QueryExecution qe = AGQueryExecutionFactory.create(sparql, this.infModel);
		long evictStartTime = System.currentTimeMillis();
		ResultSet resultSet = qe.execSelect();
		long evictEndTime = System.currentTimeMillis();
		this.metricRecorder.print((evictEndTime - evictStartTime) + ", ");
		
		ArrayList<String> results = new ArrayList<String> ();
		while (resultSet.hasNext()) {
			System.out.println("[info] sparql result: " + resultSet.next());
			String result = resultSet.next().toString();
			int length = result.length();
			results.add(result.substring(3, length - 1));
		}
		qe.close();	
		this.fMeasureBench(results);
		this.infModel.close();
	}
	
	// evict
	public boolean evict(){
		if(this.evictTime > this.evictTimes) {
			this.evictTime = 1;
			return false;
		} else {this.evictTime++;}
		System.out.println("[INFO] AGFIFO Evicting, cache size = " + this.size + " evict amount = " + this.evictAmount); 
		for(String x : this.cacheContentOfGraphIds) {
			System.out.println(x);
		}
		String queryString = "drop graph <";
		for(int i = 0; i < this.evictAmount; i++){
			String graph = this.cacheContentOfGraphIds.remove();
			//System.out.println(graph);
			queryString = queryString + graph+">; drop graph <";
		}
		queryString += ">;";
		System.out.println(queryString);
		AGQuery sparql = AGQueryFactory.create(queryString);
		QueryExecution qe = AGQueryExecutionFactory.create(sparql, model);
		qe.execAsk();
		qe.close();
		return true;
	}
	
	// f-measure benchmark
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
	
	static void printRows(StmtIterator rows) throws Exception {
		while (rows.hasNext()) {
			System.out.println(rows.next());
		}
		rows.close();
	}
	
	static void close(AGRepositoryConnection conn) {
		try {
			conn.close();
		} catch (Exception e) {
			System.err.println("Error closing repository connection: " + e);
			e.printStackTrace();
		}
	}

	private static List<AGRepositoryConnection> toClose = new ArrayList<AGRepositoryConnection>();

	protected static void closeBeforeExit(AGRepositoryConnection conn) {
		toClose.add(conn);
	}

	public void clean() {
		this.client.closeAll();
		while (toClose.isEmpty() == false) {
			AGRepositoryConnection conn = toClose.get(0);
			close(conn);
			while (toClose.remove(conn)) {
			}
		}
	}	
}
