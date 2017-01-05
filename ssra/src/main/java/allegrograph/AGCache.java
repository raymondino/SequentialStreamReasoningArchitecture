package allegrograph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import org.openrdf.OpenRDFException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
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
import utils.GraphIdCounterPair;

public class AGCache {
	
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
	
	// for statistics
	private long aveSparql;
	private double avePrecision;
	private double aveRecall;
	private double aveFmeasure;
	private long aveEvict;
	private long aveMem;

	private String serverURL;
	private String username;
	private String password;
	private String catalog;
	private String repository;
		
	// the client that talks to the back-end AllegroGraph triplestore
	private AGClient client;  
	private AGRepositoryConnection con;
	private AGModel model;
	private AGInfModel infModel;
	private AGGraph modelGraph;
	private AGGraphMaker graphMaker;
	private AGReasoner reasoner;
	
	// to record the metrics
	private PrintWriter metricRecorder; 
	// to read data from file for stream simulation
	private BufferedReader br; 
		
	public AGCache(int numberOfTriples_, int totalEvictTimes_, 
			HashSet<String> groundTruth_, String serverURL_, String username_, 
			String password_, String catalog_, String repository_) 
			throws Exception{
		
		this.evictAmount = 0;
		this.evictCounter = 1;
		this.totalEvictTimes = totalEvictTimes_;
		this.numberOfTriples = numberOfTriples_;
		this.toDeleteCounter = 0;
		this.dropQueryString = "";
		this.groundTruth = groundTruth_;
		
		this.serverURL = serverURL_;
		this.username = username_;
		this.password = password_;
		this.catalog = catalog_;
		this.repository = repository_;

		this.cacheContentOfGraphIds = new PriorityQueue<GraphIdCounterPair>();
		this.client = new AGClient(this.serverURL, this.catalog, 
				this.repository, this.username, this.password);
		
		this.client.connect();
		this.client.initTS();		
	}
	
	// set cache size
	public void setSize(int value) {
		this.size = value;
	}
	
	// set evict amount
	public void setEvictAmount(int amount) {
		this.evictAmount = amount;
	}
	
	// AGFEFO run
	public void run(String backgroundOntologyPath) 
			throws IOException, OpenRDFException {
		
		// clear everything for a new run
		this.cacheContentOfGraphIds.clear();
		this.client.emptyDB(); 
		
		// initialize statistics in each run
		this.avePrecision = 0d;
		this.aveRecall = 0d;
		this.aveSparql = 0l;
		this.aveEvict = 0l;
		this.aveMem = 0l;
		this.aveFmeasure = 0d;
		
		this.loadBkOnto(this.client, backgroundOntologyPath);
		
		this.loadCacheToFull();
		
		boolean noEndFile = true;
		boolean evicting = true;
		while(noEndFile && evicting) {	
			
			// sparql
			this.sparql();
			
			// cache eviction
			long evictStartTime = System.currentTimeMillis();
			evicting = this.evict();
			long evictEndTime = System.currentTimeMillis();
			this.aveEvict += (evictEndTime - evictStartTime);
			this.aveMem += (Runtime.getRuntime().totalMemory() -
					Runtime.getRuntime().freeMemory())/(1024*1024);
			
			// add data after eviction
			noEndFile = this.streamEmulation();
		}

		this.writeBench();
		this.infModel.close();
		this.model.close();
		this.metricRecorder.close();
		this.br.close();
	}
	
	// load background ontology
	public void loadBkOnto(AGClient client, String backgroundOntologyPath) 
			throws RDFParseException, RepositoryException, IOException {
		
		this.con = this.client.getAGConn(); 
		this.graphMaker = new AGGraphMaker(this.con);
		this.model = new AGModel(this.graphMaker
				.createGraph("http://fefocacheeviction.org/bkgdOnto"));
		this.model.read(new FileInputStream(backgroundOntologyPath), "");
		this.modelGraph = this.model.getGraph();
		this.reasoner = new AGReasoner("restriction");
		closeBeforeExit(this.con);		
	}
	
	// load the cache to its capacity
	public void loadCacheToFull() 
			throws IOException {
		this.graphID = 0;
		this.br = new BufferedReader(
				     new InputStreamReader(
						new FileInputStream(
						   new File("./resource/LUBM45.nt"))));
		this.streamEmulation();
	}
	
	// simualte the streaming data 
	public boolean streamEmulation() 
			throws IOException{
		String line = null;
		while(this.cacheContentOfGraphIds.size() < this.size){ 
			AGGraph graph = this.graphMaker
				.createGraph("http://fefocacheeviction.org/graph"+this.graphID);
			for(int i = 0; i < this.numberOfTriples; ++i){
				if ( (line = this.br.readLine()) != null ){
					line = line.replaceAll("<", "");
					line = line.replaceAll(">", "");
					String[] parts = line.split(" ");
					Node s = NodeFactory.createURI(parts[0]);
					Node p = NodeFactory.createURI(parts[1]);
					if (parts[2].contains("http")) {
						Node o = NodeFactory
								.createURI(parts[2]
										.substring(0, parts[2].length()));
						graph.add(new Triple(s, p, o));
					}
					else {
						Node o = NodeFactory
								.createLiteral(parts[2]
										.substring(1, parts[2].length()-1));
						graph.add(new Triple(s, p, o));
					}
				}
				else {
					return false;
				}
			}
			this.cacheContentOfGraphIds.add(new GraphIdCounterPair(
					"http://fefocacheeviction.org/graph"+(this.graphID++),
					LocalTime.now()));
			this.modelGraph = this.graphMaker
					.createUnion(this.modelGraph,graph);
		}
		return true;
	}

	// sparql
	public void sparql() 
			throws IOException, RepositoryException, QueryEvaluationException{
		
		// find all expired data to avoid them participating the query:
		this.toDeleteCounter = 0;
		this.dropQueryString = "";
		ArrayList<GraphIdCounterPair> expiredData = 
				new ArrayList<GraphIdCounterPair> ();
		LocalTime evictionTime = LocalTime.now();
		for(GraphIdCounterPair x: this.cacheContentOfGraphIds) {
			System.out.print(this.evictCounter + ", " + this.size + ", " 
		                     + this.evictAmount + ", " + x.graphId + ", " 
					         + x.arrivalTime + ", " + x.expirationTime);
			if(x.expirationTime.isBefore(evictionTime)) {
				expiredData.add(x);
				dropQueryString += "drop graph <" + x.graphId + ">;";
				System.out.println(", expired");
				++toDeleteCounter;
			} else {
				System.out.println();
			}
		}		
		System.out.println("[INFO] " + expiredData.size() + " data expired!");
		if(!expiredData.isEmpty()) {
			// delete expired data from the cache
			for(GraphIdCounterPair x: expiredData) {
				this.cacheContentOfGraphIds.remove(x);
			}
			// delete the expired data from the database
			QueryExecution qe = AGQueryExecutionFactory
					.create(AGQueryFactory.create(dropQueryString), model);
			qe.execAsk();
			qe.close();	
		}
		
		// after deleting expired data, load the cache again
		this.streamEmulation();
		System.out.println(this.reasoner.getEntailmentRegime());
		this.infModel = new AGInfModel(this.reasoner, this.model);
		String queryString = "select distinct ?s "
				+ "where { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
				+ "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#Professor>.}"
				;		
		AGRepositoryConnection conn = this.client.getAGConn();
	    TupleQuery tupleQuery = conn
	    		.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
	    tupleQuery.setIncludeInferred(true);  
		long sparqlStartTime = System.currentTimeMillis();
	    TupleQueryResult resultSet = tupleQuery.evaluate();
		long sparqlEndTime = System.currentTimeMillis();
		this.aveSparql += (sparqlEndTime - sparqlStartTime);

		ArrayList<String> results = new ArrayList<String> ();
		while (resultSet.hasNext()) {
			String result = resultSet.next().toString();
			System.out.println("result");
			int length = result.length();
			results.add(result.substring(3, length - 1));
	    }
	    resultSet.close();	
		
		this.fMeasureBench(results);
		this.infModel.close();
	}
	
	// evict
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
			AGQuery sparql = AGQueryFactory.create(dropQueryString);
			QueryExecution qe = AGQueryExecutionFactory.create(sparql, model);
			qe.execAsk();
			qe.close();	
		}
		return true;

	}
	
	// f-measure benchmark
	private void fMeasureBench(ArrayList<String> results) {
		if(results.size() == 0) {
			this.avePrecision += 0l;
			this.aveRecall += 0l;
			this.aveFmeasure += 0l;
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
			this.avePrecision += precision;
			this.aveRecall += recall;
			this.aveFmeasure += (2*precision*recall/(precision + recall));
		}
	}
	
	private void writeBench() 
			throws FileNotFoundException, UnsupportedEncodingException {
		// write benchmark files
		String benchmarkFilePath = "./resource/agFEFOdisk" + "/agFEFOdisk" 
								  + "_bench_" + this.numberOfTriples 	+ "_" 
				                  + this.size + "_" + this.evictAmount + ".csv";
		this.metricRecorder = new PrintWriter(benchmarkFilePath, "UTF-8");
		this.metricRecorder.println("sparql, precision, recall, fmeasure, "
				+ "explain, evict, memory");
		this.metricRecorder.println(this.aveSparql/(totalEvictTimes+1) + ", " 
				+ this.avePrecision/(totalEvictTimes+1)+ ", "
				+ this.aveRecall/(totalEvictTimes+1) + ", " 
				+ this.aveFmeasure/(totalEvictTimes+1) + ", "
				+ this.aveEvict/totalEvictTimes + ", " 
				+ this.aveMem/(totalEvictTimes+1));
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

	private static List<AGRepositoryConnection> toClose = 
			new ArrayList<AGRepositoryConnection>();

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

