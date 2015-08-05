package edu.buffalo.cse562.BDBIndexers;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.schema.Table;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.BDBIndexers.LineItemHelper.MyLineItemTupleBinder;
import edu.buffalo.cse562.BDBIndexers.LineItemHelper.SecondaryKeyCreator_LineItem;
import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.Operators.ScanOperator;

public class LineItemIndexCreator {
	Database myLineItemDB = null;	
	SecondaryDatabase myLineItemSecDb = null;//TODO
	
	public LineItemIndexCreator(Environment myDbEnvironment, EnvironmentConfig envConfig){		
		try {		   		 
		    DatabaseConfig dbConfig = new DatabaseConfig();
		    dbConfig.setAllowCreate(true);
		    
		    myLineItemDB = getLineItemDB(myDbEnvironment, dbConfig);
		    
			SecondaryConfig myLineItemSecConfig = new SecondaryConfig();
		    myLineItemSecConfig.setAllowCreate(true);
		    myLineItemSecConfig.setSortedDuplicates(true);
		    
		    createLineitemSecondaryDB(myDbEnvironment,myLineItemSecConfig);
		   
		    setConfig(dbConfig, myLineItemSecConfig); //SET configs to improve perfomance
		    writeIndex("lineitem");			 
		    		    		    
		    if (myLineItemSecDb != null) {
		       myLineItemSecDb.close();
		    }

		    if (myLineItemDB != null) {
		    	myLineItemDB.close(); 
		    }		    
		    
		} 
		catch (DatabaseException dbe) {
		    // Exception handling goes here
		}		
	}
		
	private void createLineitemSecondaryDB( Environment myDbEnvironment,
											SecondaryConfig myLineItemSecConfig) 
	{	    

	    TupleBinding<DataRow> MyLineItemTupleBinder = new MyLineItemTupleBinder();
	    
	    SecondaryKeyCreator lineItemSecCreator = 
	            new SecondaryKeyCreator_LineItem(MyLineItemTupleBinder);
	    
	 // Get a secondary object and set the key creator on it.
	    myLineItemSecConfig.setKeyCreator(lineItemSecCreator);
	    String secDbName = "myLineItemSecDB";
	    myLineItemSecDb = myDbEnvironment.openSecondaryDatabase(null, secDbName, myLineItemDB, 
	    		myLineItemSecConfig);
		
	}

	private void setConfig(DatabaseConfig mydbConfig,
						   SecondaryConfig mySecDBConfig)
	{
		mydbConfig.setTransactional(false);			
		mySecDBConfig.setTransactional(false);
	}
	
	private void writeIndex(String tblName) {	
		ScanOperator scanOp = new ScanOperator(new Table("", tblName)); //check tableSchema
		HashMap<String,ColumnDetail> schema = scanOp.getOutputTupleSchema();
		
		ArrayList<Datum> tuple =  scanOp.readOneTuple(); 
		
		long count = 1;
		
		while(tuple != null)
		{
			DataRow row = new DataRow(tuple);
			
			DatabaseEntry valueDBEntry = new DatabaseEntry();			    
			TupleBinding<DataRow> tupleBinder = new MyLineItemTupleBinder();
			tupleBinder.objectToEntry(row, valueDBEntry);
						
			KeyClass key;
			DatabaseEntry keyDBEntry = new DatabaseEntry();			
			TupleBinding<KeyClass> keyClassBinder = new KeyClassBinder();
			
			/*
			key = new KeyClass(generateKey(tblName, tuple, schema));
			//Doubtful if we need a Hash here from 2 columns from the same table whose combination is a primary key */
			
			key = new KeyClass(count); 
			count++;									
			keyClassBinder.objectToEntry(key, keyDBEntry);
					
			myLineItemDB.put(null, keyDBEntry, valueDBEntry);  //write to BDB
			
			tuple = scanOp.readOneTuple();
		}
	}
	
	/*
	 * Primary key from 2 columns from the same table whose combination is a primary key 
	 */
	private long generateKey(String tblName,ArrayList<Datum> tuple, HashMap<String,ColumnDetail> schema)
	{
		long uniqueKey = 0;
		switch (tblName)
		{
			case "lineitem":
				int lineNumberIndex =  schema.get("lineitem.linenumber").getIndex();
				int orderKeyIndex =  schema.get("lineitem.orderkey").getIndex();
				
			try 
			{
				uniqueKey = getPHash((tuple.get(lineNumberIndex)).getValue().toLong(), tuple.get(orderKeyIndex).getValue().toLong());
			} 
			catch (InvalidLeaf e) 
			{			
				e.printStackTrace();
			}
				break;
			case "orders":
				break;
		}
		
		return uniqueKey;
	}
	
	/* Cantar Pairing function get perfect hashes from two unique numbers
	A = a >= 0 ? 2 * a : -2 * a - 1;
	B = b >= 0 ? 2 * b : -2 * b - 1;
	(A + B) * (A + B + 1) / 2 + A;
	*/
	private long getPHash(long a ,long b){
		long A = 2*a; 
		long B = 2*b;
		
		return ((A + B) * (A+B+1) / 2 + A);		
	}
	
	public Database getLineItemDB(Environment myDbEnvironment, DatabaseConfig dbConfig ) 
	{
		return myDbEnvironment.openDatabase(null, 
							                 "lineitem", 
							                 dbConfig);
		
    }
}
