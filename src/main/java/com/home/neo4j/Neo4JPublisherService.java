package com.home.neo4j;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.home.test.Constants;
import com.home.test.FirstExample;

public class Neo4JPublisherService {

    private static Label publisherLabel = DynamicLabel.label(Constants.PUBLISHER);

    public static void pushDataIntoDatabase(GraphDatabaseService graphDb) {
        List<Map<String, Object>> pubData = new FirstExample().getData("select id,email from publisher",
                Constants.PUBLISHER.toLowerCase());

        long tms = System.currentTimeMillis();
        int count = 0;

        Transaction tx1 = graphDb.beginTx();
        graphDb.schema().indexFor(publisherLabel).on("publisher_id").create();
        tx1.success();
        tx1.close();

        Transaction tx2 = graphDb.beginTx();
        if (pubData.size() > 0) {
            for (Map<String, Object> publisher : pubData) {
                Node node = graphDb.createNode();
                node.addLabel(publisherLabel);
                node.setProperty("publisherId", publisher.get("publisher_id"));
                node.setProperty("name", publisher.get("email"));
                node.setProperty("email", publisher.get("email"));
                count++;
            }
        }
        tx2.success();
        tx2.close();
        System.out.println(count + " created. Time taken : " + (System.currentTimeMillis() - tms));

    }

    public static void fetchAllNodes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        Set<String> emailSet = new HashSet<String>();
        Transaction tx = null;
        int count = 0;
        try {
            tx = graphDb.beginTx();
            for (final Node node : GlobalGraphOperations.at(graphDb).getAllNodes()) {
                count++;
                if (node != null && node.getProperty("email") != null)
                    emailSet.add((String) node.getProperty("email"));
            }
        } finally {
            tx.success();
            tx.close();
        }
        System.out.println(count + " Fetched node count : " + emailSet.size() + "  Time taken : "
                + (System.currentTimeMillis() - tms));
    }

    public static void deleteAllNodes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        int count = 0;
        Transaction tx = null;
        try {
            tx = graphDb.beginTx();

            for (final Node node : GlobalGraphOperations.at(graphDb).getAllNodes()) {
                node.removeLabel(publisherLabel);
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

        deleteAllNodes(graphDb);

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
    
    public static void deleteIndexes(GraphDatabaseService graphDb) {
        long tms = System.currentTimeMillis();
        int count = 0;
        Transaction tx = null;
        try {
            tx = graphDb.beginTx();

            IndexManager index = graphDb.index();
            Index<Node> publisherSiteIndexes = index.forNodes("Publisher");
            publisherSiteIndexes.delete();
        } finally {
            tx.success();
            tx.close();
        }
        System.out.println("Deleted node count : " + count + "  Time taken : " + (System.currentTimeMillis() - tms));
    }

}

// http://www.beingjavaguys.com/2014/07/neo4j-graph-db-with-java.html
// http://neo4j.com/docs/stable/tutorials-java-embedded-unique-nodes.html