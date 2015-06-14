package com.home.neo4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.tooling.GlobalGraphOperations;

import com.home.neo4j.Neo4JPublisherService.RelTypes;
import com.home.test.Constants;
import com.home.test.FirstExample;

public class Neo4JPublisherAdTagService {
    private static Label publisherAdTagLabel = DynamicLabel.label(Constants.PUBLISHER_AD_TAG);

    public static void pushDataIntoDatabase(GraphDatabaseService graphDb) {
        List<Map<String, Object>> pubAdTagData = new FirstExample().getData(
                "select id,name,pub_id,site_id from publisher_site_ad limit 100000", Constants.PUBLISHER_AD_TAG.toLowerCase());
        long tms = System.currentTimeMillis();
        int count = 0;

        Transaction tx1 = graphDb.beginTx();
        graphDb.schema().indexFor(publisherAdTagLabel).on("adTagId").create();
        tx1.success();
        tx1.close();

        Transaction tx2 = graphDb.beginTx();

        if (pubAdTagData.size() > 0) {
            for (Map<String, Object> pubAdTag : pubAdTagData) {
                Node adTagNode = graphDb.createNode(publisherAdTagLabel);
                adTagNode.setProperty("adTagId", pubAdTag.get("publisheradtag_id"));
                adTagNode.setProperty("name", pubAdTag.get("name"));
                
                
                Node siteNode = graphDb.createNode(DynamicLabel.label(Constants.PUBLISHER_SITE));
                siteNode.setProperty("siteId", pubAdTag.get("site_id"));
                
                siteNode.createRelationshipTo(adTagNode, RelTypes.SITECONTAINS);
                
                Node pubNode = graphDb.createNode(DynamicLabel.label(Constants.PUBLISHER));
                pubNode.setProperty("pubId", pubAdTag.get("pub_id"));
                
                pubNode.createRelationshipTo(siteNode, RelTypes.PUBCONAINS);
                
                count++;
            }
        }

        tx2.success();
        tx2.close();
        System.out.println(count + " created. Time taken : " + (System.currentTimeMillis() - tms));
    }
    
    public static enum RelTypes implements RelationshipType {
        SITECONTAINS,PUBCONAINS
    }

    public static void main(String[] args) {
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabase("/home/pubmatic/Downloads/neo4j-community-2.2.2/data/Neo4jDB.db");

        registerShutdownHook(graphDb);

        deleteIndexes(graphDb);

        pushDataIntoDatabase(graphDb);

        // deleteAllNodes(graphDb);
        fetchAllNodes(graphDb);
    }

    public static void deleteIndexes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        int count = 0;
        Transaction tx = null;
        try {
            tx = graphDb.beginTx();

            IndexManager index = graphDb.index();
            Index<Node> publisherAdTagIndexes = index.forNodes("PublisherAdTag");
            publisherAdTagIndexes.delete();
        } finally {
            tx.success();
            tx.close();
        }
        System.out.println("Deleted node count : " + count + "  Time taken : " + (System.currentTimeMillis() - tms));
    }

    public static void deleteAllNodes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        Transaction tx = null;
        int count = 0;
        try {
            tx = graphDb.beginTx();
            for (final Node node : GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(publisherAdTagLabel)) {
                node.removeLabel(publisherAdTagLabel);
                Iterable<Relationship> allRelationships = node.getRelationships();
                for (Relationship relationship : allRelationships) {
                    relationship.delete();
                }
                node.delete();
                count++;
            }
        } finally {
            tx.success();
            tx.close();
        }
        System.out.println(count + " Deleted node count. Time taken : " + (System.currentTimeMillis() - tms));
    }

    public static void fetchAllNodes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        List<String> emailList = new ArrayList<String>();
        Transaction tx = null;
        int count = 0;
        try {
            tx = graphDb.beginTx();
            for (final Node node : GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(publisherAdTagLabel)) {
                count++;
                if (node != null && node.getProperty("name") != null)
                    emailList.add((String) node.getProperty("name"));
            }
        } finally {
            tx.success();
            tx.close();
        }
        System.out.println(count + " Fetched node count : " + emailList.size() + "  Time taken : "
                + (System.currentTimeMillis() - tms));
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();

            }
        });
    }
}
// https://github.com/orientechnologies/orientdb-docs/wiki/Java-Schema-Api
// /https://github.com/orientechnologies/orientdb-docs/wiki/Graph-Database-Tinkerpop
// http://pettergraff.blogspot.in/2014/01/getting-started-with-orientdb.html
// http://devdocs.inightmare.org/introduction-to-orientdb-graph-edition/
// https://github.com/orientechnologies/orientdb-docs/wiki/Java-Schema-Api