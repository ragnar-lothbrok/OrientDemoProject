package com.home.neo4j;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.neo4j.shell.ShellSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;

public class Neo4JServer {

    public static void main(String[] args) {
        
        BatchInserter ba = null;
//        BatchInserters.inserter
        
        GraphDatabaseAPI graphdb = (GraphDatabaseAPI) new GraphDatabaseFactory()
        .newEmbeddedDatabaseBuilder( "/home/pubmatic/Downloads/neo4j-community-2.2.2/data/Neo4jDB.db" )
        .setConfig( ShellSettings.remote_shell_enabled, "true" )
        .newGraphDatabase();
        ServerConfigurator config;
        config = new ServerConfigurator( graphdb );
        // let the server endpoint be on a custom port
        config.configuration().setProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, 7575 );

        WrappingNeoServerBootstrapper srv;
        srv = new WrappingNeoServerBootstrapper( graphdb, config );
        srv.start();
        
    }
    
}
