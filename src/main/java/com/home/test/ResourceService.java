package com.home.test;

import java.util.List;
import java.util.Map;

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class ResourceService {

    public void pushDataIntoDatabase() {
        OrientGraphNoTx graphNoTx = OrientGraphServiceFactory.getInstance().getGraph();
        List<Map<String, String>> resourceData = new FirstExample().getData("select * from acl_resource_type",
                Constants.Resource.toLowerCase());
        VertexUtility.createVertex(graphNoTx, Constants.Resource);
        if (resourceData != null) {
            for (Map<String, String> resource : resourceData) {
                if (VertexUtility.getVertex(graphNoTx, resource.get("resource_id"), "resource_id", Constants.Resource) != null) {
                    continue;
                }
                OrientVertex orientVertex = graphNoTx.addVertex("class:Resource");
                orientVertex.setProperty("resource_id", resource.get("resource_id"));
                orientVertex.setProperty("resource_name", (resource.get("name") == null ? "" : resource.get("name")));
                orientVertex.setProperty("resource_parent", (resource.get("parent_resource_type_id") == null ? "0"
                        : resource.get("parent_resource_type_id")));
            }
        }
        VertexUtility.createEdgeWithoutProperty(graphNoTx, Constants.ADUNIT);
//        VertexUtility.createEdgeBetweenVertex(graphNoTx, Constants.ADUNIT, Constants.Resource, Constants.USER,Constants.PUBLISHER);
    }

}