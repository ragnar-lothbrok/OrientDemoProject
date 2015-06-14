package com.home.test;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class OrientGraphServiceFactory {

	public static OrientGraphServiceFactory orientGraphServiceFactory = null;

	public static OrientBaseGraph orientBaseGraph = null;
	
	private static String DB_PATH = "remote:localhost/AccessControl";
	
	public static boolean isTransactional = true;

	private OrientGraphServiceFactory() {

	}

	public OrientBaseGraph getGraph() {
		return orientBaseGraph;
	}

	public static void setGraph() {
	    if(isTransactional){
	        OrientGraph graph = new OrientGraph(
                    DB_PATH,"admin","admin");
            orientBaseGraph = graph;
	    }else{
	        OrientGraphNoTx graph = new OrientGraphNoTx(
	                DB_PATH,"admin","admin");
	        graph.setRequireTransaction(false);
	        graph.getRawGraph().declareIntent(new OIntentMassiveInsert());
	        orientBaseGraph = graph;
	    }
	}

	public static OrientGraphServiceFactory getInstance() {
		if (orientGraphServiceFactory == null) {
			synchronized (OrientGraphServiceFactory.class) {
				if (orientGraphServiceFactory == null) {
					orientGraphServiceFactory = new OrientGraphServiceFactory();
					setGraph();
				}
			}
		}
		return orientGraphServiceFactory;
	}
}
