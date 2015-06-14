package com.home.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class Neo4jFactory {

    private static GraphDatabaseService neo4jGraph = null;

    public final static String NEO4JDB_PATH = "/home/pubmatic/Downloads/neo4j-community-2.2.2/data/Neo4jDB.db";

    private Neo4jFactory() {

    }

    public static GraphDatabaseService getGraphInstance() {
        if (neo4jGraph == null) {
            neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(NEO4JDB_PATH)
                    .setConfig(GraphDatabaseSettings.node_keys_indexable, "name")
                    .setConfig(GraphDatabaseSettings.relationship_keys_indexable, "name")
                    .setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
                    .setConfig(GraphDatabaseSettings.relationship_auto_indexing, "true").newGraphDatabase();
        }
        return neo4jGraph;
    }

    public static void registerShutdownHook(final GraphDatabaseService neo4jGraph) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (neo4jGraph != null)
                    neo4jGraph.shutdown();
            }
        });
    }
}
