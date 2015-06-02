package com.home.test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class ACLRoleService {
    
    public void pushDataIntoDatabase() {
        OrientGraphNoTx graphNoTx = OrientGraphServiceFactory.getInstance()
                .getGraph();
        List<Map<String, String>> roles = new FirstExample().getData(
                "select * from acl_group_type", Constants.ROLE.toLowerCase());
        VertexUtility.createVertex(graphNoTx,Constants.ROLE);
        if (roles != null) {
            for (Map<String, String> role : roles) {
                if (VertexUtility.getVertex(graphNoTx, role.get("name"),"name",Constants.ROLE) != null) {
                    System.out.println("Role already Exists.");
                    continue;
                }
                OrientVertex orientVertex = graphNoTx
                        .addVertex("class:Role");
                for (Entry<String, String> entry : role.entrySet()) {
                    orientVertex.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}
