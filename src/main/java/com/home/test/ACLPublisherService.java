package com.home.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class ACLPublisherService {
    public static void pushDataIntoDatabase() {
        OrientBaseGraph graphNoTx = null;
        try{
            graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(true);
            List<Map<String, Object>> pubData = new FirstExample().getData("select id,email from acl_user",
                    Constants.USER.toLowerCase());
        VertexUtility.dropClass(graphNoTx, Constants.USER);
        VertexUtility.dropIndex(graphNoTx, "user_idIndex");
        createClass(graphNoTx);
            OGlobalConfiguration.dumpConfiguration(System.out);
            long tms = System.currentTimeMillis();
            int count = 0;
            if (graphNoTx instanceof OrientGraph) {
//            ((OrientGraph) graphNoTx).begin();
                count = insertData(graphNoTx, pubData);
//            ((OrientGraph) graphNoTx).commit();
            }
            System.out.println("Expected Total : " + pubData.size() + "  Actual inserted :" + count + " Time Taken : "
                    + (System.currentTimeMillis() - tms) / 1000);
        }finally{
        }
    }

    private static int insertData(OrientBaseGraph graphNoTx, List<Map<String, Object>> pubData) {
        int count = 0;
        if (pubData.size() > 0) {
            for (Map<String, Object> publisher : pubData) {
                if (VertexUtility.getVertex(graphNoTx, (String) publisher.get("user_id"), "userId",
                        Constants.USER) == null) {
                    count++;
                    ((OrientGraph) graphNoTx).begin();
                    Map<Object, Object> propMap = new HashMap<Object, Object>();
                    propMap.put("userId", publisher.get("user_id"));
                    propMap.put("email", publisher.get("email"));
                    graphNoTx.addVertex("class:User", propMap);
                    ((OrientGraph) graphNoTx).commit();
                }
            }
        }
        return count;
    }

    public static void createClass(final OrientBaseGraph graphNoTx) {
        ((OrientGraph) graphNoTx).executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
            public Object call(OrientBaseGraph iArgument) {
                OClass publisherAdTag = graphNoTx.getRawGraph().getMetadata().getSchema()
                        .createClass(Constants.USER, (OClass) graphNoTx.getVertexBaseType());
                publisherAdTag.createProperty("userId", OType.LONG).setMandatory(true);
                publisherAdTag.createProperty("email", OType.STRING).setMandatory(true);
                publisherAdTag.createProperty("name", OType.STRING);
                publisherAdTag.createIndex("user_idIndex", OClass.INDEX_TYPE.UNIQUE, "userId");
                graphNoTx.getRawGraph().getMetadata().getSchema().save();
                return null;
            }
        });
    }

    public static void main(String[] args) {
        pushDataIntoDatabase();
        VertexUtility.printAllVertex(Constants.USER);
        OrientBaseGraph[] orientBaseGraphs = new OrientBaseGraph[100];
        /*for(int i=0;i<100;i++){
            orientBaseGraphs[i] = OrientGraphConnectionPool.getInstance().getOrientGraph(true);
            System.out.println(OrientGraphConnectionPool.getInstance().getAvailablePoolSize()+" "+OrientGraphConnectionPool.getInstance().getTotalPoolSize());
        }
        
        for(int i=0;i<15;i++){
            System.out.println(orientBaseGraphs[i].hashCode());
            System.out.println(OrientGraphConnectionPool.getInstance().getAvailablePoolSize()+" "+OrientGraphConnectionPool.getInstance().getTotalPoolSize());
        }*/
    }
}
//http://neo4j.com/docs/snapshot/tutorials-java-embedded-unique-nodes.html#tutorials-java-embedded-unique-get-or-create
///http://neo4j.com/docs/stable/server-configuration.html
//https://github.com/orientechnologies/orientdb/issues/2815
///http://neo4j.com/docs/stable/server-configuration.html