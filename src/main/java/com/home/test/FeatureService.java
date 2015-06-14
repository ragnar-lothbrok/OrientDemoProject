package com.home.test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class FeatureService {
    
	public void pushDataIntoDatabase() {
		OrientBaseGraph graphNoTx = OrientGraphServiceFactory.getInstance()
				.getGraph();
		List<Map<String, Object>> featureData = new FirstExample().getData(
				"select * from acl_feature", Constants.FEATURE.toLowerCase());
		VertexUtility.createVertex(graphNoTx, Constants.FEATURE);
		if (featureData != null) {
			for (Map<String, Object> featureRecord : featureData) {
				if (VertexUtility.getVertex(graphNoTx, (String)featureRecord.get("feature_name"),"feature_name",Constants.FEATURE) != null) {
					System.out.println("Feature already Exists.");
					continue;
				}
				OrientVertex orientVertex = graphNoTx
						.addVertex("class:Feature");
				for (Entry<String, Object> entry : featureRecord.entrySet()) {
					orientVertex.setProperty(entry.getKey(), entry.getValue());
				}
			}
		}
	}
}
