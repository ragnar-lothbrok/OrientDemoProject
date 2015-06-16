package com.home.neo4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.tooling.GlobalGraphOperations;

import com.home.test.Constants;
import com.home.test.FirstExample;

public class Neo4JPublisherSiteService {

    private static Label publisherSiteLabel = DynamicLabel.label(Constants.PUBLISHER_SITE);

    public static void pushDataIntoDatabase(GraphDatabaseService graphDb) {
        List<Map<String, Object>> pubData = new FirstExample().getData("select id,url from publisher_site",
                Constants.PUBLISHER_SITE.toLowerCase());

        long tms = System.currentTimeMillis();
        int count = 0;

        Transaction tx1 = graphDb.beginTx();
        graphDb.schema().indexFor(publisherSiteLabel).on("publishersite_id").create();
        tx1.success();
        tx1.close();

        Transaction tx2 = graphDb.beginTx();
        if (pubData.size() > 0) {
            for (Map<String, Object> pubSite : pubData) {
                Node node = graphDb.createNode();
                node.addLabel(publisherSiteLabel);
                node.setProperty("siteId", pubSite.get("publishersite_id"));
                node.setProperty("url", pubSite.get("url"));
                count++;
            }
        }
        tx2.success();
        tx2.close();
        System.out.println(count + " created. Time taken : " + (System.currentTimeMillis() - tms));

    }

    public static void fetchAllNodes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        List<String> emailList = new ArrayList<String>();
        Transaction tx = null;
        int count = 0;
        try {
            tx = graphDb.beginTx();
            for (final Node node : GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(publisherSiteLabel)) {
                count++;
                if (node != null && node.getProperty("url") != null)
                    emailList.add((String) node.getProperty("url"));
            }
        } finally {
            tx.success();
            tx.close();
        }
        System.out.println(count + " Fetched node count : " + emailList.size() + "  Time taken : "
                + (System.currentTimeMillis() - tms));
    }

    public static void deleteIndexes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        try (Transaction tx = graphDb.beginTx()) {
            for (IndexDefinition indexDefinition : graphDb.schema().getIndexes(publisherSiteLabel)) {
                indexDefinition.drop();
            }
            tx.success();
            tx.close();
        }
        System.out.println("  Time taken to delete index " + publisherSiteLabel + ": "
                + (System.currentTimeMillis() - tms));
    }

    public static void deleteAllNodes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        int count = 0;
        Transaction tx = null;
        try {
            tx = graphDb.beginTx();
            ResourceIterable<Node> nodes = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(publisherSiteLabel);
            if (nodes != null && nodes.iterator() != null) {
                try (ResourceIterator<Node> users = nodes.iterator()) {
                    ArrayList<Node> userNodes = new ArrayList<>();
                    while (users.hasNext()) {
                        userNodes.add(users.next());
                    }
                    for (Node tempNode : userNodes) {
                        tempNode.removeLabel(publisherSiteLabel);
                        tempNode.delete();
                        count++;
                    }
                }
            }
        } finally {
            tx.success();
            tx.close();
        }
        System.out.println("Deleted node count : " + count + "  Time taken : " + (System.currentTimeMillis() - tms));
    }

    public static void main(String args[]) {
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

    public static enum RelTypes implements RelationshipType {
        RELATE_TO;
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

// http://www.beingjavaguys.com/2014/07/neo4j-graph-db-with-java.html
// http://neo4j.com/docs/stable/tutorials-java-embedded-unique-nodes.html