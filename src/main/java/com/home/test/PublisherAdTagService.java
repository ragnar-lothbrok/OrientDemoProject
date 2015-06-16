package com.home.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class PublisherAdTagService {
    public static void pushDataIntoDatabase() {
        OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(true);
        List<Map<String, Object>> pubAdTagData = new FirstExample().getData("select id,name from publisher_site_ad limit 70000",
                Constants.PUBLISHER_AD_TAG.toLowerCase());
//        VertexUtility.dropClass(graphNoTx, Constants.PUBLISHER_AD_TAG);
//        VertexUtility.dropIndex(graphNoTx, "publisheradtag_idIndex");
//        createClass(graphNoTx);
        long tms = System.currentTimeMillis();
        int count = 0;
        if (graphNoTx instanceof OrientGraph) {
            ((OrientGraph) graphNoTx).begin();
            count = insertData(graphNoTx, pubAdTagData, count);
            ((OrientGraph) graphNoTx).commit();
        }
        System.out.println(count + " created. " + (System.currentTimeMillis() - tms) / 1000);
    }

    private static int insertData(OrientBaseGraph graphNoTx, List<Map<String, Object>> pubAdTagData, int count) {
        if (pubAdTagData.size() > 0) {
            for (Map<String, Object> pubAdTag : pubAdTagData) {
//                 ((OrientGraph) graphNoTx).begin();
                Map<Object, Object> propMap = new HashMap<Object, Object>();
                propMap.put("adTagId", pubAdTag.get("publisheradtag_id"));
                propMap.put("name", pubAdTag.get("name"));
                graphNoTx.addVertex("class:PublisherAdTag", propMap);
//                 ((OrientGraph) graphNoTx).commit();
                count++;
            }
        }
        return count;
    }

    public static void createClass(OrientBaseGraph graphNoTx) {
        ((OrientGraph) graphNoTx).executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
            public Object call(OrientBaseGraph iArgument) {
                OClass publisherAdTag = graphNoTx.getRawGraph().getMetadata().getSchema()
                        .createClass(Constants.PUBLISHER_AD_TAG, (OClass) graphNoTx.getVertexBaseType());
                publisherAdTag.createProperty("adTagId", OType.LONG).setMandatory(true);
                publisherAdTag.createProperty("name", OType.STRING).setMandatory(true);
                publisherAdTag.createIndex("publisheradtag_idIndex", OClass.INDEX_TYPE.UNIQUE, "adTagId");
                return null;
            }
        });
    }

    public static void main(String[] args) {
        try {
            pushDataIntoDatabase();
            VertexUtility.printAllVertex(Constants.PUBLISHER_AD_TAG);
//            VertexUtility.deleteAllVertex(OrientGraphServiceFactory.getInstance().getGraph(),
//                    Constants.PUBLISHER_AD_TAG);
//            VertexUtility.printAllVertex(Constants.PUBLISHER_AD_TAG);
        } finally {
            
        }
    }
}
// https://github.com/orientechnologies/orientdb-docs/wiki/Java-Schema-Api
// /https://github.com/orientechnologies/orientdb-docs/wiki/Graph-Database-Tinkerpop
// http://pettergraff.blogspot.in/2014/01/getting-started-with-orientdb.html
// http://devdocs.inightmare.org/introduction-to-orientdb-graph-edition/
// https://github.com/orientechnologies/orientdb-docs/wiki/Java-Schema-Api