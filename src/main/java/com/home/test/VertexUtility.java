package com.home.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class VertexUtility {
    
    public static void dropClass(OrientBaseGraph graph, String className) {
        try {
            ((OrientGraph) graph).executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
                public Object call(OrientBaseGraph iArgument) {
                    graph.getRawGraph().getMetadata().getSchema().dropClass(className);
                    graph.getRawGraph().getMetadata().getSchema().save();
                    System.out.println("Dropped class : "+className);
                    return null;
                }
            });
        } catch (Exception exception) {
            System.out.println("Exception occured : " + exception.getMessage());
        }
    }

    public static void dropIndex(OrientBaseGraph graph, String index) {
        try {
            ((OrientGraph) graph).executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
                public Object call(OrientBaseGraph iArgument) {
                    graph.dropIndex(index);
                    System.out.println("Dropped  index: "+index);
                    return null;
                }
            });
        } catch (Exception exception) {
            System.out.println("Exception occured : " + exception.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public static boolean createEdgeUsingSQL(OrientBaseGraph graph, String className, String[] origin,
            String[] destination) {
        try {
            List<String> params = new ArrayList<String>();
            String query = "create edge " + className + " from (select * from " + origin[0] + " where " + origin[1]
                    + "=" + origin[2] + ") to (select * from " + destination[0] + " where " + destination[1] + "="
                    + destination[2] + ")";
            query = "create edge "
                    + className
                    + " from (select * from Publisher where publisher_id=1) to (select * from Publisher where publisher_id=2)";
            Object obj = graph.getRawGraph().command(new OCommandSQL(query)).execute(params);
            if (obj != null && ((ArrayList<ODocument>) obj).size() > 0)
                return true;
        } catch (Exception exception) {
            return false;
        }
        return false;
    }
    
    public static boolean deleteAllVertex(OrientBaseGraph graph, String className){
        try {
            List<String> params = new ArrayList<String>();
            String query = "DELETE Vertex "+className;
            Object obj = graph.getRawGraph().command(new OCommandSQL(query)).execute(params);
            if (obj != null && ((ArrayList<ODocument>) obj).size() > 0)
                return true;
        } catch (Exception exception) {
            return false;
        }
        return false;
    }

    public static void dropEdge(OrientBaseGraph graph, String edge) {
        try {
            graph.dropEdgeType(edge);
        } catch (Exception exception) {
            System.out.println("Exception occured : " + exception.getMessage());
        }
    }

    public static void dropVertex(OrientBaseGraph graph, String vertex) {
        try {
            graph.dropVertexType(vertex);
        } catch (Exception exception) {
            System.out.println("Exception occured : " + exception.getMessage());
        }
    }

    public static OClass getClass(OrientBaseGraph graph, String className) {
        return graph.getRawGraph().getMetadata().getSchema().getClass(className);
    }

    public static void createEdgeWithoutProperty(OrientBaseGraph graph, String class_name) {
        if (graph.getEdgeType(class_name) == null) {
            graph.createEdgeType(class_name);
        }
    }

    public static void createVertex(OrientBaseGraph graph, String class_name) {
        if (graph.getVertexType(class_name) == null) {
            graph.createVertexType(class_name);
        }
    }

    public static boolean dropEdge(OrientBaseGraph graph, String vertex1, String vertex2, String edgeName) {
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

    public static OrientVertex getVertex(OrientBaseGraph graph, String value, String propertyName, String className) {
        OrientVertex vertex = null;
        try {
            String query = "select * from " + className + " where " + propertyName + " = '" + value + "'";
            OSQLSynchQuery<OrientVertex> qr = new OSQLSynchQuery<OrientVertex>(query);
            Iterable<OrientVertex> vertices = graph.command(qr).execute();
            Iterator<OrientVertex> iter = vertices.iterator();
            if (iter.hasNext())
                return iter.next();
        } catch (Exception exception) {
            return vertex;
        }
        return vertex;
    }

    public static boolean isDirectedEdgePresent(OrientBaseGraph graph, String vertex1, String vertex2,String className) {
        try {
            String query = "SELECT * FROM "+className+" WHERE (out = ? AND in = ? )";
            OSQLSynchQuery<ODocument> sql = new OSQLSynchQuery<ODocument>(query);
            OrientDynaElementIterable orientDynaElementIterable = graph.command(sql).execute(vertex1, vertex2);
            Iterator<Object> iter = orientDynaElementIterable.iterator();
            if (iter.hasNext()) {
                iter.next();
                return true;
            }
        } catch (Exception exception) {
            return false;
        }
        return false;
    }
    
    public static boolean isNonDirectedEdgePresent(OrientBaseGraph graph, String vertex1, String vertex2,String className) {
        try {
            String query = "SELECT * FROM "+className+" WHERE (out = ? AND in = ? ) OR (out = ? AND in = ? )";
            OSQLSynchQuery<ODocument> sql = new OSQLSynchQuery<ODocument>(query);
            OrientDynaElementIterable orientDynaElementIterable = graph.command(sql).execute(vertex1, vertex2);
            Iterator<Object> iter = orientDynaElementIterable.iterator();
            if (iter.hasNext()) {
                iter.next();
                return true;
            }
        } catch (Exception exception) {
            return false;
        }
        return false;
    }

    public static List<Map<String, String>> printAllVertex(String className) {
        List<Map<String, String>> dataList = new ArrayList<Map<String, String>>();
        OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(false);
        long startTime = System.currentTimeMillis();
        Iterable<Vertex> iterable = graphNoTx.getVerticesOfClass(className);
        System.out.println("printAllVertex >> Fetched Time : " + (System.currentTimeMillis() - startTime));
        Iterator<Vertex> iter = iterable.iterator();
        while (iter.hasNext()) {
            Map<String, String> data = new LinkedHashMap<String, String>();
            Vertex vertex = iter.next();
            for (String key : vertex.getPropertyKeys()) {
                data.put(key, vertex.getProperty(key) + "");
            }
            dataList.add(data);
        }
        System.out.println("printAllVertex >>  "+className + " : total read  " + dataList.size());
        return dataList;
    }

    public static boolean createEdgeBetweenVertex(OrientBaseGraph graph, String edgeName, String vertexClass1,
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
