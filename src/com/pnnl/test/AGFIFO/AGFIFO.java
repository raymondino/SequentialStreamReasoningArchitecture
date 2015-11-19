package com.pnnl.test.AGFIFO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.reasoner.Derivation;
import com.hp.hpl.jena.vocabulary.RDF;
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
//import com.hp.hpl.jena.query.ResultSet;

public class AGFIFO {
	
	/**
	 *  AGFIFO cache private variables
	 */
	private int cacheSize;
	private Queue<String> cache;
	private int graph_id;
	private int numberOfTriples; // number of triples in a single graph
	private int evictAmount; // how many graphs are evicted at one time
	ArrayList <Resource> results; // contains the sparql result sets
	
	/**
	 * AGFIFO AllegroGraph essentials
	 */
	private AGClient client;
	private AGRepositoryConnection con;
	private AGModel model;
	private AGInfModel infModel;
	private AGGraph modelGraph;
	private AGGraphMaker graphMaker;
	private AGReasoner reasoner;
	private BufferedReader br;
	
	/**
	 * metrics for benchmark
	 */
	private PrintWriter metricRecorder;
		
	/**
	 *  AGFIFO class contructor
	 * @throws UnsupportedEncodingException 
	 * @throws FileNotFoundException 
	 */
	public AGFIFO(int cacheSize_, Queue<String> cache_, int graph_id_, int numberOfTriples_, int evictAmount_) throws FileNotFoundException, UnsupportedEncodingException{
		this.cacheSize = cacheSize_;
		this.cache = cache_;
		this.graph_id = graph_id_;
		this.numberOfTriples = numberOfTriples_;
		this.evictAmount = evictAmount_;
		String benchmarkFilePath = "files/agfifo/agfifo_bench_"+ cacheSize_+"_"+evictAmount+".txt";
		this.metricRecorder = new PrintWriter(benchmarkFilePath, "UTF-8");
		this.metricRecorder.println("reason, sparql, evict");
	}
	
	/**
	 * main function: run command line: java AGFIFO cacheSize_ evictAmount_
	 * @param args
	 * @throws Exception
	 */
//	public static void main (String[] args) throws Exception{
	public void exec() throws Exception {	
		// lazy person: I know I need to check the argument input, but I just want to leave it as it is. 
		// make sure your argument is correct when use this program.
		int cacheSize_ = 1000;
		int evictAmount_ = 100;
		System.out.println("Running with cache size = " + (cacheSize_ + 1) + ", evict amount = " + evictAmount_);

		/**
		 * Step 1: define the cache & initialize an AGFIFO object
		 */
		int graph_id_ = 0;
		int numberOfTriples_ = 4;
		// use LinkedList to hold the graph names, so that to index and delete graphs in triple store will be easier. 
		Queue<String> cache= new LinkedList<String>();
		AGFIFO FIFO = new AGFIFO(cacheSize_, cache, graph_id_, numberOfTriples_, evictAmount_);
		FIFO.run(FIFO);	
		System.out.println("Done with cache size = " + cacheSize_ + ", evict amount = " + evictAmount_);
		System.out.println("--------------------------------------------------------------------------");
	}
	
	/**
	 *  run function
	 */
	public void run (AGFIFO FIFO) throws Exception{
		
		/**
		 *  Step 2 & 3?g
		 */
		FIFO.Initialize(FIFO);
		
		/**
		 * Step 4: load the cache to its full caoacity
		 */	
		long loadCacheStartTime = System.currentTimeMillis();
		FIFO.LoadCache(FIFO);
		long loadCacheEndTime = System.currentTimeMillis();
		FIFO.metricRecorder.println("CacheLoad," + (loadCacheEndTime - loadCacheStartTime));
		
		/**
		 * Step 5: emulating the data stream, evicting graphs in the cache, reasoning & sparql query
		 */
		FIFO.process(FIFO);
		
		// close all connections to clean up
		FIFO.infModel.close();
		FIFO.model.close();
		FIFO.metricRecorder.close();
		FIFO.br.close();
		closeAll();
	}
	
	/**
	 * Initialize function 
	 * @param FIFO
	 * @throws Exception
	 */
	public void Initialize(AGFIFO FIFO) throws Exception{
		/**
		 *  Step 2: connect to the AllegroGraph server
		 */
		FIFO.client = new AGClient("http://localhost:10035", "system", "FIFO", "rui", "yr");
		FIFO.client.connect();
		FIFO.client.initTS();
		FIFO.results = new ArrayList<Resource>();
		
		/**
		 * Step 3: load the background ontology
		 */
		FIFO.con = FIFO.client.getAGConn(); closeBeforeExit(FIFO.con);
		FIFO.graphMaker = new AGGraphMaker(FIFO.con);
		FIFO.model = new AGModel(FIFO.graphMaker.createGraph("http://fifocacheeviction.org/bkgdOnto"));
		FIFO.model.read(new FileInputStream("files/univ-bench.owl"), "");
		FIFO.modelGraph = FIFO.model.getGraph();
		FIFO.reasoner = new AGReasoner("restriction");
		//FIFO.reasoner.setDerivationLogging(true); // for tracing back the trples involved in the reasoning.
	}
	
	/**
	 * Load the cache to its full capacity
	 * @param FIFO
	 * @throws IOException
	 */
	public void LoadCache(AGFIFO FIFO) throws IOException{
		FIFO.br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("files/LUBMSmallFull.nt"))));
		FIFO.StreamEmulation(FIFO);
		FIFO.model.add(new AGModel(modelGraph));
	}
	
	/**
	 * process reasoning, sparql & eviction
	 * @param FIFO
	 * @throws Exception 
	 */
	public void process(AGFIFO FIFO) throws Exception{
		boolean flag = true;
		while(flag){
			// reason
			long reasonStartTime = System.currentTimeMillis();
			FIFO.Reason(FIFO);
			long reasonEndTime = System.currentTimeMillis();
			FIFO.metricRecorder.print((reasonEndTime - reasonStartTime)+",");
			
			// sparql
			long sparqlStartTime = System.currentTimeMillis();
			FIFO.Sparql(FIFO);
			long sparqlEndTime = System.currentTimeMillis();
			FIFO.metricRecorder.print((sparqlEndTime - sparqlStartTime)+",");
			
			// explain
			FIFO.Explain(FIFO);
			
			FIFO.infModel.close(); // This is crucial.
			                       // If not closed, the memory will be consumed quickly to freeze everything. 
			                       // Also more processes will be created which carves up the cpu time			
			// cache eviction
			long evictStartTime = System.currentTimeMillis();
			FIFO.Evict(FIFO);
			long evictEndTime = System.currentTimeMillis();
			FIFO.metricRecorder.print((evictEndTime - evictStartTime));
			FIFO.metricRecorder.println();
			
			// add data after eviction
			flag = FIFO.StreamEmulation(FIFO); // if the end of the file, system stops
			FIFO.metricRecorder.flush();
		}			
	}
	
	/**
	 * emulate a stream from a file by reading it line by line
	 * @param FIFO
	 * @throws IOException
	 */
	public boolean StreamEmulation(AGFIFO FIFO) throws IOException{
		String line = null;
		while(FIFO.cache.size() < FIFO.cacheSize){ // that means you can read and feed the data
			AGGraph graph = FIFO.graphMaker.createGraph("http://fifocacheeviction.org/graph"+FIFO.graph_id);
			for(int i = 0; i < FIFO.numberOfTriples; ++i){
				if ( (line = FIFO.br.readLine()) != null ){
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
			FIFO.cache.add("http://fifocacheeviction.org/graph"+FIFO.graph_id);
			FIFO.graph_id = FIFO.graph_id + 1;
			FIFO.modelGraph = FIFO.graphMaker.createUnion(FIFO.modelGraph,graph);
		}
		return true;
	}
	
	/**
	 * execute reasoning
	 * @param FIFO
	 * @throws Exception 
	 */
	public void Reason(AGFIFO FIFO) throws Exception{
		FIFO.infModel = new AGInfModel(FIFO.reasoner, FIFO.model);
	}

	/**
	 * execute sparql
	 * @param FIFO
	 */
	public void Sparql(AGFIFO FIFO){
		//String queryString = "select distinct ?s where { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#Chair>}";
		String queryString = "select distinct ?s where { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#FullProfessor>, <http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person>}";
		AGQuery sparql = AGQueryFactory.create(queryString);
		QueryExecution qe = AGQueryExecutionFactory.create(sparql, FIFO.infModel);
		//qe.execSelect();
		//qe.close();	
		FIFO.results.clear();
		try {
			ResultSet results = qe.execSelect();
			while (results.hasNext()) {
				System.out.println("[info] sparql result: " + results.next());
				String result = results.next().toString();
				int length = result.length();
				FIFO.results.add(FIFO.model.createResource(result.substring(3, length - 1)));
			}
		} finally {
			qe.close();
		}
	}
	
	/**
	 * execute FIFO eviction
	 * @param FIFO
	 */
	public void Evict(AGFIFO FIFO){
		//System.out.print("[info] evicting: ");
		String queryString = "drop graph <";
		for(int i = 0; i < FIFO.evictAmount; i++){
			String graph = FIFO.cache.remove();
			//System.out.println(graph);
			queryString = queryString + graph+">; drop graph <";
		}
		queryString += ">;";
		AGQuery sparql = AGQueryFactory.create(queryString);
		QueryExecution qe = AGQueryExecutionFactory.create(sparql, model);
		qe.execAsk();
		qe.close();
	}
	
	public void Explain(AGFIFO FIFO) {
		final PrintWriter out = new PrintWriter(System.out);
		Resource person = FIFO.model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person");
		for(Resource s : FIFO.results){
			for(StmtIterator i = FIFO.infModel.listStatements(s, RDF.type, person); i.hasNext();){
				Statement st = i.nextStatement();
				System.out.println("Entailment is " + st);
				for(final Iterator<Derivation> id = FIFO.infModel.getDerivation(st); id.hasNext(); ) {
					final Derivation deriv = (Derivation) id.next();
					deriv.printTrace(out, true);
				}
			}
		}
		out.flush();
		out.close();
	}
	
	/**
	 *  helper functions to clean up everything:
	 */
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

	/**
	 * This is just a quick mechanism to make sure all connections get closed.
	 */
	protected static void closeBeforeExit(AGRepositoryConnection conn) {
		toClose.add(conn);
	}

	protected static void closeAll() {
		while (toClose.isEmpty() == false) {
			AGRepositoryConnection conn = toClose.get(0);
			close(conn);
			while (toClose.remove(conn)) {
			}
		}
	}	
}
