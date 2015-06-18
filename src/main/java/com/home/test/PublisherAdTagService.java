package com.home.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class PublisherAdTagService {
    
    static{
        int mb = 1024*1024;
        
        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
         
        System.out.println("##### Heap utilization statistics [MB] #####");
         
        //Print used memory
        System.out.println("Used Memory:"
            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
 
        //Print free memory
        System.out.println("Free Memory:"
            + runtime.freeMemory() / mb);
         
        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);
 
        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
    }

   /* static{
        try {
            System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("/home/pubmatic/git/pizzaconnections/OrientClassDiagramCreator/src/main/java/generated/orient.log",true))));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    */
    
    public static void pushDataIntoDatabase() {
        List<Map<String, Object>> pubAdTagData = new FirstExample().getData(
                "select id,name from publisher_site_ad limit 255000", Constants.PUBLISHER_AD_TAG.toLowerCase());
        OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(false);
        graphNoTx.declareIntent(new OIntentMassiveInsert());
        VertexUtility.dropClass(graphNoTx, Constants.PUBLISHER_AD_TAG);
        VertexUtility.dropIndex(graphNoTx, "publisheradtag_idIndex");
        createClass(graphNoTx);
        long tms = System.currentTimeMillis();
        int count = 0;
        if (graphNoTx instanceof OrientGraph) {
//             ((OrientGraph) graphNoTx).begin();
            count = insertData(graphNoTx, pubAdTagData, count);
//             ((OrientGraph) graphNoTx).commit();
        } else {
            count = insertData(graphNoTx, pubAdTagData, count);
        }
        graphNoTx.declareIntent(null);
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

    public static void createClass(final OrientBaseGraph graphNoTx) {
        if (graphNoTx instanceof OrientGraph) {
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
        } else {
            OClass publisherAdTag = graphNoTx.getRawGraph().getMetadata().getSchema()
                    .createClass(Constants.PUBLISHER_AD_TAG, (OClass) graphNoTx.getVertexBaseType());
            publisherAdTag.createProperty("adTagId", OType.LONG).setMandatory(true);
            publisherAdTag.createProperty("name", OType.STRING).setMandatory(true);
            publisherAdTag.createIndex("publisheradtag_idIndex", OClass.INDEX_TYPE.UNIQUE, "adTagId");
        }
    }

    public static void main(String[] args) {
        try {
//            pushDataIntoDatabase();
            VertexUtility.printAllVertex(Constants.PUBLISHER_AD_TAG);
//            VertexUtility.deleteAllVertex(OrientGraphConnectionPool.getInstance().getOrientGraph(true),
//                    Constants.PUBLISHER_AD_TAG,"publisheradtag_idIndex");
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