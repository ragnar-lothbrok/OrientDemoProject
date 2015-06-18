package com.home.neo4j;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import com.home.test.Constants;
import com.home.test.FirstExample;

public class Neo4JEdgePublisherSiteAdTagService {

    private static Label publisherAdTagLabel = DynamicLabel.label(Constants.PUBLISHER_AD_TAG);

    private static Label publisherLabel = DynamicLabel.label(Constants.PUBLISHER);

    private static Label publisherSiteLabel = DynamicLabel.label(Constants.PUBLISHER_SITE);

    public static enum RelTypes implements RelationshipType {
        SITECONTAINS, PUBCONAINS
    }

    public static void pushDataIntoDatabase(GraphDatabaseService graphDb) {
        List<Map<String, Object>> pubAdTagSiteData = new FirstExample().getData(
                "select id,name,site_id,pub_id from publisher_site_ad limit 7000,2500",
                Constants.PUBLISHER_AD_TAG.toLowerCase());
        int count = 0;
        long tms = System.currentTimeMillis();
        Transaction tx1 = graphDb.beginTx();
        Relationship relationship = null;
        if (pubAdTagSiteData != null) {
            for (Map<String, Object> pubSiteAdTag : pubAdTagSiteData) {
                Node publisherNode = getNode(graphDb, publisherLabel, "publisherId",
                        (String) pubSiteAdTag.get("pub_id"));
                Node publisherSiteNode = getNode(graphDb, publisherSiteLabel, "siteId",
                        (String) pubSiteAdTag.get("site_id"));
                Node publisherAdTagNode = getNode(graphDb, publisherAdTagLabel, "adTagId",
                        (String) pubSiteAdTag.get("publisheradtag_id"));

               /* if (publisherNode != null && publisherSiteNode != null
                        && !isRelationShipPresent(publisherNode, publisherSiteNode, RelTypes.SITECONTAINS)) {
                    Transaction tx2 = graphDb.beginTx();
                    relationship = publisherNode.createRelationshipTo(publisherSiteNode, RelTypes.SITECONTAINS);
                    if (relationship != null)
                        relationship.setProperty("name", pubSiteAdTag.get("name"));
                    count++;
                    tx2.success();
                    tx2.close();
                }*/

                if (publisherAdTagNode != null && publisherSiteNode != null
                        && !isRelationShipPresent(publisherSiteNode, publisherAdTagNode, RelTypes.PUBCONAINS)) {
//                    Transaction tx3 = graphDb.beginTx();
                    publisherSiteNode.createRelationshipTo(publisherAdTagNode, RelTypes.PUBCONAINS);
                    if (relationship != null)
                        relationship.setProperty("name", pubSiteAdTag.get("name"));
                    count++;
//                    tx3.success();
//                    tx3.close();
                }
            }
        }
        tx1.success();
        tx1.close();
        System.out.println("Edges created : " + count + " Time Taken : " + (System.currentTimeMillis() - tms));
    }

    public static boolean deleteRelationShipsBetweenNodes(GraphDatabaseService graphDb, Node origin, Node destination,
            RelTypes reltypes) {
        Transaction tx = null;
        try {
            tx = graphDb.beginTx();
            if (origin != null && destination != null && reltypes != null) {
                Iterable<Relationship> firstRel = origin.getRelationships(reltypes, Direction.OUTGOING);
                if (firstRel != null && firstRel.iterator() != null) {
                    Iterator<Relationship> relIterator = firstRel.iterator();
                    while (relIterator.hasNext()) {
                        Relationship relationship = relIterator.next();
                        Node cNode = relIterator.next().getEndNode();
                        if (cNode.equals(destination)) {
                            relationship.delete();
                            return true;
                        }
                    }
                }
            }
        } finally {
            if (tx != null) {
                tx.success();
                tx.close();
            }
        }
        return false;
    }

    public static void deleteAllRelationShips(GraphDatabaseService graphDb, Label labelName) {
        Transaction tx = null;
        int count = 0;
        long tms = System.currentTimeMillis();
        try {
            tx = graphDb.beginTx();
            ResourceIterable<Node> nodes = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(labelName);
            if (nodes != null && nodes.iterator() != null) {
                ResourceIterator<Node> users = nodes.iterator();
                while (users.hasNext()) {
                    Iterable<Relationship> firstRel = users.next().getRelationships();
                    if (firstRel != null && firstRel.iterator() != null) {
                        Iterator<Relationship> relIterator = firstRel.iterator();
                        while (relIterator.hasNext()) {
                            relIterator.next().delete();
                            count++;
                        }
                    }
                }
            }
        } finally {
            if (tx != null) {
                tx.success();
                tx.close();
            }
        }
        System.out.println("Edges deleted : " + count + " Time Taken : " + (System.currentTimeMillis() - tms));
    }

    public static void getAllRelationShips(GraphDatabaseService graphDb) {
        getAllRelationShipByLabel(graphDb, publisherLabel);
        getAllRelationShipByLabel(graphDb, publisherSiteLabel);
    }

    public static void deleteAllRelationShips(GraphDatabaseService graphDb) {
        deleteAllRelationShips(graphDb, publisherLabel);
        deleteAllRelationShips(graphDb, publisherSiteLabel);
    }

    public static void getAllRelationShipByLabel(GraphDatabaseService graphDb, Label labelName) {
        Transaction tx = null;
        int count = 0;
        long tms = System.currentTimeMillis();
        try {
            tx = graphDb.beginTx();
            ResourceIterable<Node> nodes = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(labelName);
            if (nodes != null && nodes.iterator() != null) {
                ResourceIterator<Node> users = nodes.iterator();
                while (users.hasNext()) {
                    Iterable<Relationship> firstRel = users.next().getRelationships();
                    if (firstRel != null && firstRel.iterator() != null) {
                        Iterator<Relationship> relIterator = firstRel.iterator();
                        while (relIterator.hasNext()) {
                            relIterator.next();
                            count++;
                        }
                    }
                }
            }
        } finally {
            if (tx != null) {
                tx.success();
                tx.close();
            }
        }
        System.out.println("Total Edges :  " + labelName + " : " + count + " Time Taken : "
                + (System.currentTimeMillis() - tms));
    }

    public static boolean isRelationShipPresent(Node origin, Node destination, RelTypes reltypes) {
        if (origin != null && destination != null && reltypes != null) {
            Iterable<Relationship> firstRel = origin.getRelationships(reltypes, Direction.OUTGOING);
            if (firstRel != null && firstRel.iterator() != null) {
                Iterator<Relationship> relIterator = firstRel.iterator();
                while (relIterator.hasNext()) {
                    Node cNode = relIterator.next().getEndNode();
                    if (cNode.equals(destination)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static Node getNode(GraphDatabaseService graphDb, Label nodeLabel, String propName, String propValue) {
        Node node = null;
        Transaction tx = graphDb.beginTx();
        try {
            ResourceIterable<Node> nodes = graphDb.findNodesByLabelAndProperty(nodeLabel, propName, propValue);
            if (nodes != null && nodes.iterator() != null) {
                ResourceIterator<Node> users = nodes.iterator();
                while (users.hasNext()) {
                    node = users.next();
                    break;
                }
            }
        } finally {
            if (tx != null) {
                tx.success();
                tx.close();
            }
        }
        return node;
    }

    public static void main(String[] args) {
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabase("/home/pubmatic/Downloads/neo4j-community-2.2.2/data/Neo4jDB.db");
//        getAllRelationShips(graphDb);
        deleteAllRelationShips(graphDb);
        pushDataIntoDatabase(graphDb);
    }
}
