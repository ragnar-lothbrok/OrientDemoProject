package com.home.test;

public enum Permission {

	READ(1), WRITE(2);

	private int value;

	Permission(int value) {
		this.value = value;
	}
	
	public static String getPermissionValue(int value){
		for(Permission permission : Permission.values()){
			if(permission.value == value){
				return permission.name();
			}
		}
		return null;
	}
}
