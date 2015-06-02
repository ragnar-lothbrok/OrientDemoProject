package com.home.test;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class OrientGraphServiceFactory {

	public static OrientGraphServiceFactory orientGraphServiceFactory = null;

	public static OrientGraphNoTx orientGraphNoTx = null;

	private OrientGraphServiceFactory() {

	}

	public OrientGraphNoTx getGraph() {
		return orientGraphNoTx;
	}

	public static void setGraph() {
		OrientGraphNoTx graph = new OrientGraphNoTx(
				"plocal:/home/pubmatic/Downloads/orientdb-community-2.0.10/databases/AccessControl");
		graph.setRequireTransaction(false);
		graph.getRawGraph().declareIntent(new OIntentMassiveInsert());
		orientGraphNoTx = graph;
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
