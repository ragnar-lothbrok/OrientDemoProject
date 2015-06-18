package com.home.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class PublisherSiteService {
    public static void pushDataIntoDatabase() {
        OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(true);
        List<Map<String, Object>> pubSiteData = new FirstExample().getData("select id,url from publisher_site",
                Constants.PUBLISHER_SITE.toLowerCase());
        VertexUtility.dropClass(graphNoTx, Constants.PUBLISHER_SITE);
        VertexUtility.dropIndex(graphNoTx, "publishersite_idIndex");
        createClass(graphNoTx);
        long tms = System.currentTimeMillis();
        int count = 0;
        if (graphNoTx instanceof OrientGraph) {
            ((OrientGraph) graphNoTx).begin();
            count = insertData(graphNoTx, pubSiteData);
            ((OrientGraph) graphNoTx).commit();
        }
        System.out.println(count + " created. " + (System.currentTimeMillis() - tms) / 1000);
    }

    private static int insertData(OrientBaseGraph graphNoTx, List<Map<String, Object>> pubSiteData) {
        int count = 0;
        if (pubSiteData.size() > 0) {
            for (Map<String, Object> pubSite : pubSiteData) {
                if (VertexUtility.getVertex(graphNoTx, (String) pubSite.get("publishersite_id"), "siteId",
                        Constants.PUBLISHER_SITE) == null) {
                    Map<Object, Object> propMap = new HashMap<Object, Object>();
                    propMap.put("siteId",pubSite.get("publishersite_id"));
                    propMap.put("url",pubSite.get("url"));
                    graphNoTx.addVertex("class:PublisherSite",propMap);
                    count++;
                }
            }
        }
        return count;
    }

    public static void createClass(final OrientBaseGraph graphNoTx) {
        ((OrientGraph) graphNoTx).executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
            public Object call(OrientBaseGraph iArgument) {
                OClass publisherAdTag = graphNoTx.getRawGraph().getMetadata().getSchema()
                        .createClass(Constants.PUBLISHER_SITE, (OClass) graphNoTx.getVertexBaseType());
                publisherAdTag.createProperty("siteId", OType.LONG).setMandatory(true);
                publisherAdTag.createProperty("url", OType.STRING).setMandatory(true);
                publisherAdTag.createIndex("publishersite_idIndex", OClass.INDEX_TYPE.UNIQUE, "siteId");
                graphNoTx.getRawGraph().getMetadata().getSchema().save();
                return null;
            }
        });
    }

    public static void main(String[] args) {
//        pushDataIntoDatabase();
        VertexUtility.printAllVertex(Constants.PUBLISHER_SITE);
    }
}
