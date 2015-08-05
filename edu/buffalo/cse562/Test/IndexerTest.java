package edu.buffalo.cse562.Test;

import java.io.File;
import java.util.ArrayList;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryDatabase;

import edu.buffalo.cse562.BDBIndexers.KeyClass;
import edu.buffalo.cse562.BDBIndexers.KeyClassBinder;
import edu.buffalo.cse562.BDBIndexers.LineItemHelper.MyLineItemTupleBinder;
import edu.buffalo.cse562.BDBIndexers.OrdersHelper.MyOrdersTupleBinder;
import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;

public class IndexerTest {
	EnvironmentConfig envConfig1 = new EnvironmentConfig();
	Environment myDbEnvironment1 = new Environment(new File("db"), 
            						  			  envConfig1);
    String tblName = "orders";
    DatabaseConfig dbConfig1 = new DatabaseConfig();
	Database myLineItemDB = myDbEnvironment1.openDatabase(null, 
												        tblName, 
												        dbConfig1);
    	
	public IndexerTest(){
		
	long aKey = 1;
	
	for(aKey = 1;aKey < 102; aKey++){

	try {
	    // Create a pair of DatabaseEntry objects. theKey
	    // is used to perform the search. theData is used
	    // to store the data returned by the get() operation.
	
	    DatabaseEntry theKey = new DatabaseEntry();
	    DatabaseEntry theData = new DatabaseEntry();
	    
		KeyClass key = new KeyClass(aKey);
		KeyClassBinder kB = new KeyClassBinder();
		kB.objectToEntry(key, theKey);
	    
	    // Perform the get.
	    if (myLineItemDB.get(null, theKey, theData, LockMode.READ_UNCOMMITTED) ==
	        OperationStatus.SUCCESS) 
	    {
	        // Recreate the data String.
	        //byte[] retData = theData.getData();
//	    	 String foundData = new String(retData, "UTF-8");
//		     System.out.println("For key: '" + aKey + "' found data: '" + 
//		                            foundData + "'.");	 
	    	
	    	TupleBinding<DataRow> tupleBinder = null;
	    	DataRow dr = null;
	    	
	    	switch (tblName)
	    	{
	    	case "orders":
	    		tupleBinder = new MyOrdersTupleBinder();

	    		break;
	    	case "lineitem":
	    		tupleBinder = new MyLineItemTupleBinder();
	    		break;
	    	case "customers":
	    		//TODO
	    		break;
	    	}	        
			
    		dr = tupleBinder.entryToObject(theData);
    		
			ArrayList<Datum> li = dr.getRow();
			
			String s = "";
			for(Datum da : li)
			{				
				s = s+da.toString()+"|";						
			}				       
			
			System.out.println(s);
			
	    } else {
	        System.out.println("No record found for key '" + aKey + "'.");
	    } 
	} catch (Exception e) {
	    // Exception handling goes here
	}
	
	}
	
	}
}
