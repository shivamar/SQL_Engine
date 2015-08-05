package edu.buffalo.cse562.BDBIndexers;

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class GeneralIndexCreator {
	Environment myDbEnvironment = null;
	EnvironmentConfig envConfig = null;
	
	/*
	 * Set envConfig.setLocking(false);
	 * Set db and secDB transaction(false);
	 */
	
	String envLocation = "";
	
	public GeneralIndexCreator(String dbDirLocation)
	{	
		envLocation = dbDirLocation;		
	    setEnvironment();
		createDataBaseIndexes();
		closeEnvironment();
	}	
	
	private void setEnvironment()
	{
		envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(true);
	   // envConfig.setLocking(false);  
	    myDbEnvironment = new Environment(new File(envLocation), 
	                                      envConfig);
	}
	
	private void createDataBaseIndexes()	
	{
		new LineItemIndexCreator(myDbEnvironment, envConfig);
		new OrdersIndexer(myDbEnvironment, envConfig);
	}
	
	private void closeEnvironment(){
		myDbEnvironment.close();
	}	
}
