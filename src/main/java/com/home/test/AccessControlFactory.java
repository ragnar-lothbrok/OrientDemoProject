package com.home.test;


public class AccessControlFactory {

	public static void main(String[] args) {
		
//		new FeatureService().pushDataIntoDatabase();
		
		VertexUtility.printAllVertex(Constants.FEATURE);
//		
//		new FeatureGroupService().pushDataIntoDatabase();
		
		VertexUtility.printAllVertex(Constants.FEATURE_GROUP);
		new FeatureGroupService().printAllEdges();
		
//		new FeatureGroupMappingService().pushDataIntoDatabase();
//		new FeatureGroupMappingService().printAllEdges();
//		
//		new ACLRoleService().pushDataIntoDatabase();
		VertexUtility.printAllVertex(Constants.ROLE);
		
//		new UserTeamService().pushDataIntoDatabase();
//		VertexUtility.printAllVertex(Constants.Team);
//		VertexUtility.printAllVertex(Constants.USER);
		
//		new ResourceService().pushDataIntoDatabase();
		VertexUtility.printAllVertex(Constants.Resource);
		
		new TempService().pushDataIntoDatabase();
		
	}
	
}
