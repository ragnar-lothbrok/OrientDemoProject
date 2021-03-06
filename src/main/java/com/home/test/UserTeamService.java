package com.home.test;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class UserTeamService {

    public void pushDataIntoDatabase() {
        OrientBaseGraph graphNoTx = OrientGraphConnectionPool.getInstance().getOrientGraph(true);
        List<Map<String, Object>> userTeamMappingData = new FirstExample()
                .getData(
                        "select ag.group_type_id,au.id as user_id,au.first_name as first_name,au.last_name as last_name,au.email as email from acl_group_user_mapping agcm inner join acl_group ag on ag.id=agcm.group_id inner join acl_user au on au.id=agcm.user_id",
                        null);
        OrientVertex roleVertex = null;
        OrientVertex userVertex = null;
        OrientVertex teamVertex = null;
        OrientVertex resourceVertex = null;
        VertexUtility.createVertex(graphNoTx,Constants.USER);
        VertexUtility.createVertex(graphNoTx,Constants.Team);
        resourceVertex = VertexUtility.getVertex(graphNoTx, Constants.PUBLISHER, "resource_name", Constants.Resource);
        if (userTeamMappingData != null) {
            for (Map<String, Object> userRecord : userTeamMappingData) {
                roleVertex = VertexUtility.getVertex(graphNoTx, (String)userRecord.get("group_type_id"), "role_id",Constants.ROLE);
                userVertex = VertexUtility.getVertex(graphNoTx, (String)userRecord.get("user_id"), "user_id",Constants.USER);
                teamVertex = VertexUtility.getVertex(graphNoTx, "team_"+userRecord.get("user_id"), "team_id",Constants.Team);
                teamVertex = VertexUtility.getVertex(graphNoTx, "team_"+userRecord.get("user_id"), "team_id",Constants.Team);
                if(userVertex == null){
                    if(userRecord.get("user_id") != null){
                        OrientVertex orientVertex = graphNoTx.addVertex("class:User");
                        orientVertex.setProperty("user_id", userRecord.get("user_id"));
                        orientVertex.setProperty("first_name", userRecord.get("first_name") == null ? "":userRecord.get("first_name"));
                        orientVertex.setProperty("last_name", (userRecord.get("last_name") == null ? "":userRecord.get("last_name")));
                        orientVertex.setProperty("email", userRecord.get("email"));
                        userVertex = orientVertex;
                    }
                }
                
                resourceVertex = VertexUtility.getVertex(graphNoTx, Constants.PUBLISHER, "resource_name", Constants.Resource);
                if(resourceVertex != null && userVertex != null && !VertexUtility.isDirectedEdgePresent(graphNoTx, resourceVertex.getIdentity().toString(), userVertex.getIdentity().toString(),Constants.HAS)){
                    VertexUtility.createEdgeWithoutProperty(graphNoTx, Constants.HAS);
                    resourceVertex.addEdge(Constants.HAS, userVertex, null, null);
                }
                
                if(teamVertex == null){
                    if(userRecord.get("user_id") != null){
                        OrientVertex orientVertex = graphNoTx.addVertex("class:Team");
                        orientVertex.setProperty("team_id", "team_"+userRecord.get("user_id"));
                        orientVertex.setProperty("team_name", "team_"+userRecord.get("email"));
                        teamVertex = orientVertex;
                    }
                }
                createEdge(Constants.HAS, graphNoTx);
                if(teamVertex != null && roleVertex != null && !VertexUtility.isDirectedEdgePresent(graphNoTx, teamVertex.getIdentity().toString(), roleVertex.getIdentity().toString(),Constants.HAS)){
                    Calendar calendar = Calendar.getInstance();
                    Date startDate = calendar.getTime();
                    calendar.setTimeInMillis(startDate.getTime()+10000);
                    teamVertex.addEdge(Constants.HAS, roleVertex, null, null, "startDate", calendar.getTime(),"endDate", calendar.getTime());
                }
            }
        }

    }
    
    public void createEdge(String edgeName,OrientBaseGraph graph){
        if (graph.getEdgeType(edgeName) == null) {
            OrientEdgeType orientEdgeType = graph.createEdgeType(edgeName);
            orientEdgeType.createProperty("startDate", OType.DATETIME);
            orientEdgeType.createProperty("endDate", OType.DATETIME);
        }
    }

}
