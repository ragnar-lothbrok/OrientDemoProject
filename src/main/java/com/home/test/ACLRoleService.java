package com.home.test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class ACLRoleService {
    
    public void pushDataIntoDatabase() {
        OrientBaseGraph graphNoTx = OrientGraphServiceFactory.getInstance()
                .getGraph();
        List<Map<String, Object>> roles = new FirstExample().getData(
                "select * from acl_group_type", Constants.ROLE.toLowerCase());
        VertexUtility.createVertex(graphNoTx,Constants.ROLE);
        if (roles != null) {
            for (Map<String, Object> role : roles) {
                if (VertexUtility.getVertex(graphNoTx, (String)role.get("name"),"name",Constants.ROLE) != null) {
                    System.out.println("Role already Exists.");
                    continue;
                }
                OrientVertex orientVertex = graphNoTx
                        .addVertex("class:Role");
                for (Entry<String, Object> entry : role.entrySet()) {
                    orientVertex.setProperty(entry.getKey(), (String)entry.getValue());
                }
            }
        }
    }
}
