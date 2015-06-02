package com.home.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class VertexUtility {

    public static void createEdgeWithoutProperty(OrientGraphNoTx graph, String class_name) {
        if (graph.getEdgeType(class_name) == null) {
            graph.createEdgeType(class_name);
        }
    }

    public static void createVertex(OrientGraphNoTx graph, String class_name) {
        if (graph.getVertexType(class_name) == null) {
            graph.createVertexType(class_name);
        }
    }

    public static OrientVertex getVertex(OrientGraphNoTx graph, String value, String propertyName, String className) {
        OrientVertex orientVertex = null;
        Iterable<Vertex> queryVertex = (Iterable<Vertex>) graph.query().has(propertyName, Compare.EQUAL, value)
                .has("@class", className).vertices();
        Iterator<Vertex> iter = queryVertex.iterator();
        // If class has already been created returns the imported vertex
        if (queryVertex != null && iter != null && iter.hasNext())
            orientVertex = ((OrientVertex) iter.next());
        return orientVertex;
    }

    public static boolean dropEdge(OrientGraphNoTx graph, String vertex1, String vertex2, String edgeName) {
        try {
            List<String> params = new ArrayList<String>();
            String query = "delete edge " + edgeName + " from " + vertex1 + " to " + vertex2;
            int deleted = graph.getRawGraph().command(new OCommandSQL(query)).execute(params);
            if (deleted > 0)
                return true;
        } catch (Exception exception) {
            return false;
        }
        return false;
    }

    public static boolean isEdgePresent(OrientGraphNoTx graph, String vertex1, String vertex2) {
        try {
            String query = "SELECT label FROM E WHERE (out = ? AND in = ? ) OR (out = ? AND in = ? )";
            OSQLSynchQuery<ODocument> sql = new OSQLSynchQuery<ODocument>(query);
            OrientDynaElementIterable orientDynaElementIterable = graph.command(sql).execute(vertex1, vertex2);
            Iterator<Object> iter = orientDynaElementIterable.iterator();
            if (iter.hasNext())
                return true;
        } catch (Exception exception) {
            return false;
        }
        return false;
    }

    public static List<Map<String, String>> printAllVertex(String className) {
        List<Map<String, String>> dataList = new ArrayList<Map<String, String>>();
        OrientGraphNoTx graphNoTx = OrientGraphServiceFactory.getInstance().getGraph();
        Iterable<Vertex> iterable = graphNoTx.getVerticesOfClass(className);
        Iterator<Vertex> iter = iterable.iterator();
        while (iter.hasNext()) {
            Map<String, String> data = new LinkedHashMap<String, String>();
            Vertex vertex = iter.next();
            for (String key : vertex.getPropertyKeys()) {
                data.put(key, vertex.getProperty(key));
            }
            dataList.add(data);
        }
        System.out.println(className + " : " + dataList);
        return dataList;
    }

    public static boolean createEdgeBetweenVertex(OrientGraphNoTx graph, String edgeName, String vertexClass1,
            String vertexClass2, String resource_name) {
        try {
            String query = "create edge " + edgeName + " from (select from " + vertexClass1 + " where resource_name='"
                    + resource_name + "') to (select from " + vertexClass2 + ")";
            graph.getRawGraph().command(new OCommandSQL(query)).execute();
        } catch (Exception exception) {
            return false;
        }
        return false;
    }

}
