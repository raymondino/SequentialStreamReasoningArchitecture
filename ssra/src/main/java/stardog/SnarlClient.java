package stardog;

import org.openrdf.model.Model;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.complexible.common.rdf.model.Values;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.complexible.stardog.api.reasoning.ReasoningConnection;
import com.complexible.stardog.db.DatabaseOptions;

public class SnarlClient {
	private AdminConnection adminConn;
	private ReasoningConnection aConn;
	private String serverURL;
	private String dbName;
	private String password;
	private String username;
	private String cacheType;
	
	// constructor
	public SnarlClient(String serverURL_, String cacheType_, String dbName_, 
			String username_, String password_) {
		serverURL = serverURL_;
		dbName = dbName_;
		username = username_;
		password = password_;
		cacheType = cacheType_;
		
		adminConn = AdminConnectionConfiguration
				.toServer(serverURL)
				.credentials(username, password)
				.connect();
		
		emptyDB();
		
		aConn = ConnectionConfiguration
				.to(dbName)
				.server(serverURL)
				.credentials(username, password)
				.reasoning(true)
				.connect()
				.as(ReasoningConnection.class);
		
		System.out.println("[INFO] connection to " + serverURL + "/" 
		                   + dbName + " is established");
	}
	
	// connection getter
	public ReasoningConnection getAConn(){
		return aConn;
	}
	
	// add model
	public void addModel(Model m, String graph_id) {
		aConn.begin();
		aConn.add().graph(m, Values.iri(graph_id));
		aConn.commit();
	}
	
	// triples count
	public void countTriples() throws QueryEvaluationException {
		TupleQueryResult aResult = aConn
				.select("select (count (distinct *) as ?count) "
						+ "where { graph ?g {?s ?p ?o.} }").execute();
		String count = aResult.next().getBinding("count").toString();
		aResult.close();
		System.out.println("[info] " + count + " triples in the database " 
		                   + dbName);
	}
	
	// empty triplestore
	public void emptyDB() {
		if(adminConn.list().contains(dbName)) {
			adminConn.drop(dbName);
		}
		if(cacheType.equals("disk")) {
			adminConn			
		    .disk(dbName)
		    .set(DatabaseOptions.QUERY_ALL_GRAPHS, true)
		    .create();			
		}
		else {
			adminConn			
		    .memory(dbName)
		    .set(DatabaseOptions.QUERY_ALL_GRAPHS, true)
		    .create();
		}
	}
	
	// drop triplestore
	public void dropDB() {
		adminConn.drop(dbName);
	}
	
	// clean up everything
	public void cleanUp() {
		adminConn.close();
		aConn.close();
	}
}
