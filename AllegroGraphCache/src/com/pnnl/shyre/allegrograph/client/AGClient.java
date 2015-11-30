package com.pnnl.shyre.allegrograph.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.openrdf.OpenRDFException;
import org.openrdf.repository.RepositoryException;


//import com.franz.agraph.jena.AGGraph;
import com.franz.agraph.jena.AGGraphMaker;
import com.franz.agraph.jena.AGInfModel;
import com.franz.agraph.jena.AGModel;
import com.franz.agraph.jena.AGQuery;
import com.franz.agraph.jena.AGQueryExecutionFactory;
import com.franz.agraph.jena.AGQueryFactory;
import com.franz.agraph.jena.AGReasoner;
import com.franz.agraph.repository.AGCatalog;
import com.franz.agraph.repository.AGRepository;
import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;

public class AGClient {
	
	// connections for AllegroGraph manipulation
	private AGServer agServer;
	private AGCatalog agCatalog;
	private AGRepository agRepository;
	private AGRepositoryConnection agConn;
	private AGGraphMaker agMaker;
	private AGModel agModel;
	private AGReasoner agReasoner;
	private AGInfModel infModel;	
	
	// credentials to access AllegroGraph server
	private String server;
	private String catalog;
	private String repository;
	private String entailmentRegime;
	private HashMap<String, String> credentials;	
	
	// agCliend class constructor
	public AGClient(String server, String catalog, String repository,
			String username, String password) throws Exception{
		
		this.server = server;
		this.catalog = catalog;
		this.repository = repository;
		this.entailmentRegime = "restriction"; 
		
		// credentials to access the server
		this.credentials = new HashMap<String, String>();
		this.credentials.put("username", username);
		this.credentials.put("password", password);		
	}
		
	// get allegrograph connection
	public AGRepositoryConnection getAGConn() throws RepositoryException{
		return this.agRepository.getConnection();
	} 
	
	// non-inference model
	public AGModel getAGModel(){
		return (new AGModel((new AGGraphMaker(agConn).getGraph())));
	}
	
	// inference model
	public AGInfModel getInfModel() {
		agMaker = new AGGraphMaker(agConn);
		agModel = new AGModel(agMaker.getGraph());
		agReasoner = new AGReasoner(entailmentRegime);
		//System.out.println(agReasoner.getEntailmentRegime());
		infModel = new AGInfModel(agReasoner, agModel);
		return infModel;
	}
	
	// setter method
	public void setReasoningLevel(String r) {
		switch(r.toLowerCase()) {
		case "restriction":
			this.entailmentRegime = "restriction";
			break;		
		default:
			this.entailmentRegime = "rdfs++";
			break;
		}
	}
	
	// Connect to AllegroGraph Server and initialize repository
	public void connect() throws Exception {
		println("[INFO] Connecting to AllegroGraph Server : " + this.server);
		agServer = new AGServer(this.server, 
				this.credentials.get("username"), 
				this.credentials.get("password"));		
		
		agCatalog = agServer.getCatalog(this.catalog);
		println("[INFO] Connection established");
	}
	
	// Initialize Repository (Triple Store)
	public void initTS() throws Exception {
		println("[INFO] Creating background triple store : " + this.repository);		
		if(agCatalog.listRepositories().contains(this.repository)){
			closeAll();
			agCatalog.deleteRepository(this.repository);
		}			
		agRepository = agCatalog.createRepository(this.repository);
		agRepository.initialize();
		println("[INFO] Initialized Repository: " + this.repository);
		agConn = agRepository.getConnection();
		println("[INFO] Established Repository Connection");
		closeBeforeExit(agConn);
	}
	
	public void emptyDB() throws RepositoryException, OpenRDFException {
		if(agCatalog.listRepositories().contains(this.repository)){
			closeAll();
			agCatalog.deleteRepository(this.repository);
		}			
		agRepository = agCatalog.createRepository(this.repository);
		agRepository.initialize();
		println("[INFO] Initialized Repository: " + this.repository);
		agConn = agRepository.getConnection();
		println("[INFO] Established Repository Connection");
		closeBeforeExit(agConn);
	}
	
	// get triple count
	public void getTripleCount() {
		agMaker = new AGGraphMaker(agConn);
		agModel = new AGModel(agMaker.getGraph());
		try {
			// COUNT QUERY FOR REPORTING TO UI
			String queryString = "SELECT (COUNT(*) AS ?count) { SELECT DISTINCT * { ?s ?p ?o } }";
			AGQuery sparql = AGQueryFactory.create(queryString);
			
			// RUN QUERY AND GET RESULTS
			QueryExecution qe = AGQueryExecutionFactory.create(sparql, agModel);
			try {
				ResultSet results = qe.execSelect();
				QuerySolution result = results.next();
				
				// FETCH THE COUNT FROM THE EXECUTION QUERY
				Literal count = result.getLiteral("count");
				long countLong = count.getLong();
				System.out.println("[INFO] Current triples in " + this.repository + " : " + countLong);
			} finally {
				qe.close();
			}
		} finally {
			agModel.close();

		}
	}
	
	// helper function: print out to console
	public static void println(Object x) {
		System.out.println(x);
	}

	// a list containing all connections to be closed
	private static List<AGRepositoryConnection> toClose = new ArrayList<AGRepositoryConnection>();

	// helper function: collect connections to be closed before exit the program
	private static void closeBeforeExit(AGRepositoryConnection conn) {
		toClose.add(conn);
	}

	// helper function: close connection of AllegroGraph server
	private void close(AGRepositoryConnection conn) {
		try {
			conn.close();
		} catch (Exception e) {
			System.err.println("Error closing repository connection: " + e);
			e.printStackTrace();
		}
	}
	
	// helper function: close all connections to AllegroGraph server in toClose list
	public void closeAll() {
		while (toClose.isEmpty() == false) {
			AGRepositoryConnection conn = toClose.get(0);
			close(conn);
			while (toClose.remove(conn)) {
			}
		}
	}
	
}
