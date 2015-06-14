package com.home.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class PublisherService {
    public static void pushDataIntoDatabase() {
        OrientBaseGraph graphNoTx = OrientGraphServiceFactory.getInstance().getGraph();
        List<Map<String, Object>> pubData = new FirstExample().getData("select id,email from publisher",
                Constants.PUBLISHER.toLowerCase());
        VertexUtility.dropClass(graphNoTx, Constants.PUBLISHER);
        VertexUtility.dropIndex(graphNoTx, "publisher_idIndex");
        createClass(graphNoTx);
        long tms = System.currentTimeMillis();
        int count = 0;
        if (graphNoTx instanceof OrientGraph) {
            ((OrientGraph) graphNoTx).begin();
            count = insertData(graphNoTx, pubData);
            ((OrientGraph) graphNoTx).commit();
        }
        System.out.println("Expected Total : " + pubData.size() + "  Actual inserted :" + count + " Time Taken : "
                + (System.currentTimeMillis() - tms) / 1000);
    }

    private static int insertData(OrientBaseGraph graphNoTx, List<Map<String, Object>> pubData) {
        int count = 0;
        if (pubData.size() > 0) {
            for (Map<String, Object> publisher : pubData) {
                if (VertexUtility.getVertex(graphNoTx, (String) publisher.get("publisher_id"), "publisherId",
                        Constants.PUBLISHER) == null) {
                    count++;
                    Map<Object, Object> propMap = new HashMap<Object, Object>();
                    propMap.put("publisherId", publisher.get("publisher_id"));
                    propMap.put("email", publisher.get("email"));
                    propMap.put("name", "arvind" + count);
                    graphNoTx.addVertex("class:Publisher", propMap);
                }
            }
        }
        return count;
    }

    public static void createClass(OrientBaseGraph graphNoTx) {
        ((OrientGraph) graphNoTx).executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
            public Object call(OrientBaseGraph iArgument) {
                OClass publisherAdTag = graphNoTx.getRawGraph().getMetadata().getSchema()
                        .createClass(Constants.PUBLISHER, (OClass) graphNoTx.getVertexBaseType());
                publisherAdTag.createProperty("publisherId", OType.LONG).setMandatory(true);
                publisherAdTag.createProperty("email", OType.STRING).setMandatory(true);
                publisherAdTag.createProperty("name", OType.STRING);
                publisherAdTag.createIndex("publisher_idIndex", OClass.INDEX_TYPE.UNIQUE, "publisherId");
                graphNoTx.getRawGraph().getMetadata().getSchema().save();
                return null;
            }
        });
    }

    public static void main(String[] args) {
        pushDataIntoDatabase();
        VertexUtility.printAllVertex(Constants.PUBLISHER);
    }
}
//http://neo4j.com/docs/snapshot/tutorials-java-embedded-unique-nodes.html#tutorials-java-embedded-unique-get-or-create
///http://neo4j.com/docs/stable/server-configuration.html
//https://github.com/orientechnologies/orientdb/issues/2815
///http://neo4j.com/docs/stable/server-configuration.html