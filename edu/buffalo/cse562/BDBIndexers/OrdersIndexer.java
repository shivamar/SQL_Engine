package edu.buffalo.cse562.BDBIndexers;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.schema.Table;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.BDBIndexers.LineItemHelper.MyLineItemTupleBinder;
import edu.buffalo.cse562.BDBIndexers.LineItemHelper.SecondaryKeyCreator_LineItem;
import edu.buffalo.cse562.BDBIndexers.OrdersHelper.MyOrdersTupleBinder;
import edu.buffalo.cse562.BDBIndexers.OrdersHelper.SecondaryKeyCreator_Orders;
import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.Operators.ScanOperator;

public class OrdersIndexer {
	Database myOrdersDB = null;	
	SecondaryDatabase myOrdersSecDb = null;
	/*   orders.orderdate=orderdate DATE  0, 
   		 orders.orderkey=orderkey INT  1, 
   		 orders.custkey=custkey INT REFERENCES CUSTOMER 2,
   		 orders.shippriority=shippriority INT  3}
	 */
	
	public OrdersIndexer(Environment myDbEnvironment, EnvironmentConfig envConfig){	
	    DatabaseConfig dbConfig = new DatabaseConfig();
	    dbConfig.setAllowCreate(true);
	    
	    myOrdersDB = getOrdersDB(myDbEnvironment, dbConfig);
	    
		SecondaryConfig myOrdersSecConfig = new SecondaryConfig();
		myOrdersSecConfig.setAllowCreate(true);
		myOrdersSecConfig.setSortedDuplicates(true);
		
		createOrdersSecondaryDB(myDbEnvironment, myOrdersSecConfig); 
		
		setConfig(dbConfig, myOrdersSecConfig);
		
		writeIndex("orders");			 
	    
	    if (myOrdersSecDb != null) {
	    	myOrdersSecDb.close();
	    }

	    if (myOrdersDB != null) {
	    	myOrdersDB.close(); 
	    }		    
		    
	}

	private void writeIndex(String tblName) {
		ScanOperator scanOp = new ScanOperator(new Table("", tblName)); //check tableSchema
		HashMap<String,ColumnDetail> schema = scanOp.getOutputTupleSchema();
		
		ArrayList<Datum> tuple =  scanOp.readOneTuple(); 
				
		while(tuple != null)
		{
			DataRow row = new DataRow(tuple);
			
			DatabaseEntry valueDBEntry = new DatabaseEntry();			    
			TupleBinding<DataRow> tupleBinder = new MyOrdersTupleBinder();
			tupleBinder.objectToEntry(row, valueDBEntry);
						
			KeyClass key = null;
			DatabaseEntry keyDBEntry = new DatabaseEntry();			
			TupleBinding<KeyClass> keyClassBinder = new KeyClassBinder();
			
			try 
			{
				key = new KeyClass(tuple.get(1).getValue().toLong()); //depends on row order from Current Schema of Orders
			} catch (InvalidLeaf e)
			{
				e.printStackTrace();
			}
			
			keyClassBinder.objectToEntry(key, keyDBEntry);
					
			myOrdersDB.put(null, keyDBEntry, valueDBEntry);  //write to BDB
			
			tuple = scanOp.readOneTuple();
		}
		
	}

	private void setConfig(DatabaseConfig dbConfig,
						   SecondaryConfig myOrdersSecConfig) 
	{
		dbConfig.setTransactional(false);			
		myOrdersSecConfig.setTransactional(false);		
	}

	private void createOrdersSecondaryDB(Environment myDbEnvironment,
			SecondaryConfig myOrdersSecConfig) {
	  TupleBinding<DataRow> MyOrdersTupleBinder = new MyOrdersTupleBinder();
	    
	    SecondaryKeyCreator secKeyCreator_Orders = 
	            new SecondaryKeyCreator_Orders(MyOrdersTupleBinder);
	    
	 // Get a secondary object and set the key creator on it.
	    myOrdersSecConfig.setKeyCreator(secKeyCreator_Orders);
	    String secDbName = "myLineItemSecDB";
	    myOrdersSecDb = myDbEnvironment.openSecondaryDatabase(null, secDbName, myOrdersDB, 
	    														myOrdersSecConfig);		
	}

	private Database getOrdersDB(Environment myDbEnvironment,
								 DatabaseConfig dbConfig) 
	{	
		return myDbEnvironment.openDatabase(null, 
                "orders", 
                dbConfig);
	}
}
