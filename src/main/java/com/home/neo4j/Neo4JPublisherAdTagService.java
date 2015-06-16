package com.home.neo4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.tooling.GlobalGraphOperations;

import com.home.test.Constants;
import com.home.test.FirstExample;

public class Neo4JPublisherAdTagService {
    private static Label publisherAdTagLabel = DynamicLabel.label(Constants.PUBLISHER_AD_TAG);

    public static void pushDataIntoDatabase(GraphDatabaseService graphDb) {
        List<Map<String, Object>> pubAdTagData = new FirstExample().getData(
                "select id,name,pub_id,site_id from publisher_site_ad", Constants.PUBLISHER_AD_TAG.toLowerCase());
        long tms = System.currentTimeMillis();
        int count = 0;

        Transaction tx1 = graphDb.beginTx();
        try {
            graphDb.schema().indexFor(publisherAdTagLabel).on("adTagId").create();
        } catch (Exception exception) {
        }
        tx1.success();
        tx1.close();

        Transaction tx2 = graphDb.beginTx();

        if (pubAdTagData.size() > 0) {
            for (Map<String, Object> pubAdTag : pubAdTagData) {
                Node adTagNode = graphDb.createNode(publisherAdTagLabel);
                adTagNode.setProperty("adTagId", pubAdTag.get("publisheradtag_id"));
                adTagNode.setProperty("name", pubAdTag.get("name"));
                count++;
            }
        }

        tx2.success();
        tx2.close();
        System.out.println(count + " created. Time taken : " + (System.currentTimeMillis() - tms));
    }

    public static void main(String[] args) {
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabase("/home/pubmatic/Downloads/neo4j-community-2.2.2/data/Neo4jDB.db");

        registerShutdownHook(graphDb);

        fetchAllNodes(graphDb);

        deleteIndexes(graphDb);

        fetchAllNodes(graphDb);

        deleteAllNodes(graphDb);

        pushDataIntoDatabase(graphDb);

        fetchAllNodes(graphDb);
    }

    public static void deleteIndexes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        try (Transaction tx = graphDb.beginTx()) {
            for (IndexDefinition indexDefinition : graphDb.schema().getIndexes(publisherAdTagLabel)) {
                indexDefinition.drop();
            }
            tx.success();
            tx.close();
        }
        System.out.println("Time taken to delete index " + publisherAdTagLabel + ": "
                + (System.currentTimeMillis() - tms));
    }

    public static void deleteAllNodes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        Transaction tx = null;
        int count = 0;
        try {
            tx = graphDb.beginTx();
            ResourceIterable<Node> nodes = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(publisherAdTagLabel);
            if (nodes != null && nodes.iterator() != null) {
                try (ResourceIterator<Node> users = nodes.iterator()) {
                    ArrayList<Node> userNodes = new ArrayList<>();
                    while (users.hasNext()) {
                        userNodes.add(users.next());
                    }
                    for (Node tempNode : userNodes) {
                        tempNode.removeLabel(publisherAdTagLabel);
                        tempNode.delete();
                        count++;
                    }
                }
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