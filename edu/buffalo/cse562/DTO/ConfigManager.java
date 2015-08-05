package edu.buffalo.cse562.DTO;

public class ConfigManager {
	private static String DATA_DIR;
	private static String STATIC_DIR;
	private static String DB_DIR;
	public static boolean envirSet;

	public static void setDataDir(String dir){
		DATA_DIR = dir;
	}

	public static String getDataDir(){
		return DATA_DIR;
	}

	public static void setDBDir(String dir){
		DB_DIR = dir;
	}

	public static String getDbDir() {
		return DB_DIR;
	}

	public static void setSwapDir(String swDir){
		STATIC_DIR = swDir;
	}
	
	public static String getSwapDir(){
		return STATIC_DIR;
	}
}
