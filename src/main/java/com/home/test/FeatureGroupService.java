package com.home.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class FeatureGroupService {
    
    public void pushDataIntoDatabase() {
        OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(true);
        List<Map<String, Object>> featureData = new FirstExample().getData("select * from acl_group",
                Constants.FEATURE_GROUP.toLowerCase());
        VertexUtility.createVertex(graphNoTx,Constants.FEATURE_GROUP);
        OrientVertex roleVertex = null;
        OrientVertex featureGroupVertex = null;
        if (featureData != null) {
            for (Map<String, Object> featureRecord : featureData) {
                featureGroupVertex = VertexUtility.getVertex(graphNoTx, (String)featureRecord.get("group_name"), "group_name",Constants.FEATURE_GROUP);
                if (featureGroupVertex != null) {
                    System.out.println("Feature Group already Exists.");
                }else{
                    OrientVertex orientVertex = graphNoTx.addVertex("class:FeatureGroup");
                    for (Entry<String, Object> entry : featureRecord.entrySet()) {
                        if (!"group_type_id".equalsIgnoreCase(entry.getKey()))
                            orientVertex.setProperty(entry.getKey(), entry.getValue());
                    }
                }
                featureGroupVertex = VertexUtility.getVertex(graphNoTx, (String)featureRecord.get("group_name"), "group_name",Constants.FEATURE_GROUP);
                roleVertex = VertexUtility.getVertex(graphNoTx, (String)featureRecord.get("group_type_id"), "role_id",Constants.ROLE);
                if(featureGroupVertex != null && roleVertex != null && !VertexUtility.isDirectedEdgePresent(graphNoTx,roleVertex.getIdentity().toString(), featureGroupVertex.getIdentity().toString(),"contains")){
                    VertexUtility.createEdgeWithoutProperty(graphNoTx, "contains");
                    roleVertex.addEdge("contains", featureGroupVertex);
                }
            }
        }
    }

    public List<Map<String, String>> printAllEdges() {
        List<Map<String, String>> dataList = new ArrayList<Map<String, String>>();
        OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(false);
        Iterable<Edge> iterable = graphNoTx.getEdgesOfClass("contains");
        Iterator<Edge> iter = iterable.iterator();
        while (iter.hasNext()) {
            Map<String, String> data = new LinkedHashMap<String, String>();
            Edge edge = iter.next();
            Vertex roleVertex = edge.getVertex(Direction.OUT);
            Vertex featureGroupVertex = edge.getVertex(Direction.IN);
            if (roleVertex != null) {
                for (String key : roleVertex.getPropertyKeys()) {
                    if (key.equalsIgnoreCase("role_id")) {
                        data.put(key, (String) roleVertex.getProperty(key));
                        break;
                    }
                }
            }
            if (featureGroupVertex != null) {
                for (String key : featureGroupVertex.getPropertyKeys()) {
                    if (key.equalsIgnoreCase("featuregroup_id")) {
                        data.put(key, (String) featureGroupVertex.getProperty(key));
                        break;
                    }
                }
            }
            for (String key : edge.getPropertyKeys()) {
                data.put(key, (String) edge.getProperty(key));
            }
            dataList.add(data);
        }
        System.out.println("Role Edges : "+dataList);
        return dataList;
    }
}
