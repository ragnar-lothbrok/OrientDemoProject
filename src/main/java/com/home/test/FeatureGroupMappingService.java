package com.home.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class FeatureGroupMappingService {
    
    private void createEdge(OrientBaseGraph graph, String class_name) {
        // graph.dropEdgeType(class_name);
        if (graph.getEdgeType(class_name) == null) {
            OrientEdgeType orientEdgeType = graph.createEdgeType(class_name);
            orientEdgeType.createProperty("permission", OType.STRING);
        }
    }

    public void pushDataIntoDatabase() {
        OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(true);
        List<Map<String, Object>> featureData = new FirstExample().getData("select * from acl_group_feature_mapping",
                Constants.FEATUREGROUPMAPPING.toLowerCase());
        createEdge(graphNoTx, Constants.FEATUREGROUPMAPPING);

        if (featureData != null) {
            for (Map<String, Object> featureRecord : featureData) {
                OrientVertex featureVertex = null;
                OrientVertex featureGroupVertex = null;
                String permission = null;
                for (Entry<String, Object> entry : featureRecord.entrySet()) {
                    if ("feature_id".equalsIgnoreCase(entry.getKey()))
                        featureVertex = VertexUtility.getVertex(graphNoTx, (String)entry.getValue(),
                                "Feature_id".toLowerCase(),Constants.FEATURE);
                    if ("group_id".equalsIgnoreCase(entry.getKey()))
                        featureGroupVertex = VertexUtility.getVertex(graphNoTx, (String)entry.getValue(),
                                "FeatureGroup_id".toLowerCase(),Constants.FEATURE_GROUP);
                    if ("permission_id".equalsIgnoreCase(entry.getKey()))
                        permission = Permission.getPermissionValue(Integer.parseInt((String)entry.getValue()));
                }
                if(!VertexUtility.isDirectedEdgePresent(graphNoTx, featureVertex.getIdentity().toString(), featureGroupVertex.getIdentity().toString(),"FeatureGroupMapping")){
                    featureGroupVertex.addEdge("FeatureGroupMapping", featureVertex, null, null, "permission", permission);
                }
            }
        }

    }

    public List<Map<String, String>> printAllEdges() {
        List<Map<String, String>> dataList = new ArrayList<Map<String, String>>();
        OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(false);
        Iterable<Edge> iterable = graphNoTx.getEdgesOfClass("FeatureGroupMapping");
        Iterator<Edge> iter = iterable.iterator();
        while (iter.hasNext()) {
            Map<String, String> data = new LinkedHashMap<String, String>();
            Edge edge = iter.next();
            Vertex featureGroupVertex = edge.getVertex(Direction.OUT);
            Vertex featureVertex = edge.getVertex(Direction.IN);
            if (featureGroupVertex != null) {
                for (String key : featureGroupVertex.getPropertyKeys()) {
                    if (key.equalsIgnoreCase("featuregroup_id")) {
                        data.put(key, (String) featureGroupVertex.getProperty(key));
                        break;
                    }
                }
            }
            if (featureVertex != null) {
                for (String key : featureVertex.getPropertyKeys()) {
                    if (key.equalsIgnoreCase("feature_id")) {
                        data.put(key, (String) featureVertex.getProperty(key));
                        break;
                    }
                }
            }
            for (String key : edge.getPropertyKeys()) {
                data.put(key, (String) edge.getProperty(key));
            }
            dataList.add(data);
        }
        System.out.println(dataList);
        return dataList;
    }
}
