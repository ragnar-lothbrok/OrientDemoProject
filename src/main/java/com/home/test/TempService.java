package com.home.test;

import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TempService {

    public void pushDataIntoDatabase() {
        OrientBaseGraph graphNoTx = OrientGraphServiceFactory.getInstance().getGraph();
        String team_id = "team_25";
        OrientVertex parentVertex = VertexUtility.getVertex(graphNoTx, team_id, "team_id", Constants.Team);
        OrientVertex orientVertex = null;
        VertexUtility.createEdgeWithoutProperty(graphNoTx, "hasTeamChild");
        //
        // orientVertex = graphNoTx.addVertex("class:Team");
        // orientVertex.setProperty("team_id", "team_xyz");
        // parentVertex.addEdge("hasTeamChild", orientVertex);
        //
        // orientVertex = graphNoTx.addVertex("class:Team");
        // orientVertex.setProperty("team_id", "team_abc");
        // parentVertex.addEdge("hasTeamChild", orientVertex);
        //
        // orientVertex = graphNoTx.addVertex("class:Team");
        // orientVertex.setProperty("team_id", "team_pqr");
        // parentVertex.addEdge("hasTeamChild", orientVertex);

        parentVertex = VertexUtility.getVertex(graphNoTx, "team_pqr", "team_id", Constants.Team);
        orientVertex = graphNoTx.addVertex("class:Team");
        orientVertex.setProperty("team_id", "team_pqrs");
        parentVertex.addEdge("hasTeamChild", orientVertex);

    }

}
