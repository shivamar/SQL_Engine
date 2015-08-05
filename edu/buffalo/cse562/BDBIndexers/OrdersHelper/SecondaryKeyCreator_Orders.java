package edu.buffalo.cse562.BDBIndexers.OrdersHelper;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;

public class SecondaryKeyCreator_Orders implements SecondaryKeyCreator{
private TupleBinding<DataRow> theBinding;
	
	public SecondaryKeyCreator_Orders(
			TupleBinding<DataRow> myOrdersTupleBinder_SecKey) {
		theBinding = myOrdersTupleBinder_SecKey;
	}

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb,
            DatabaseEntry keyEntry, 
            DatabaseEntry dataEntry,
            DatabaseEntry resultEntry) {		
	/*  Current Schema Orders!!
	/*   orders.orderdate=orderdate DATE  0, 
   		 orders.orderkey=orderkey INT  1, 
   		 orders.custkey=custkey INT REFERENCES CUSTOMER 2,
   		 orders.shippriority=shippriority INT  3}
	 */	
		
		DataRow row = (DataRow) theBinding.entryToObject(dataEntry);
		Datum orderdateDatum = row.getRow().get(0);	// depends on row order from Current Schema of Orders
		String ordersDate = orderdateDatum.getValue().toString();
			 																		
		ByteArrayOutputStream out = new ByteArrayOutputStream(); //not using tuple bind here just for one column!
		DataOutputStream dataOut = new DataOutputStream(out);
		
		try 
		{
			dataOut.writeUTF(ordersDate);
			resultEntry.setData(out.toByteArray());
		} 
		catch (IOException willNeverOccur) 
		{
			willNeverOccur.printStackTrace();
		}

		return true;
	}
}
