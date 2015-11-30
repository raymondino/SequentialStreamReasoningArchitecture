/*
 * @Author Rui Yan
 * @Usage: for adding data to database only
 */

package com.pnnl.shyre.utilities;

import org.openrdf.model.Model;
import org.openrdf.model.Resource;

public class Graph {
	public Model m;
	public Resource id;

	
	public Graph (Model m_, Resource id_) {
		m = m_;
		id = id_;
	}
}
