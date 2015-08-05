package edu.buffalo.cse562.BDBIndexers.LineItemHelper;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;

public class SecondaryKeyCreator_LineItem implements SecondaryKeyCreator {
	private TupleBinding<DataRow> theBinding;
	
	public SecondaryKeyCreator_LineItem(
			TupleBinding<DataRow> myLineItemTupleBinder_SecKey) {
		theBinding = myLineItemTupleBinder_SecKey;
	}

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb,
            DatabaseEntry keyEntry, 
            DatabaseEntry dataEntry,
            DatabaseEntry resultEntry) {		
		/*  Current Schema LineItem!!
		 * lineitem.linenumber=linenumber INT  0, 
		 * lineitem.shipdate=shipdate DATE  1, 
		 * lineitem.orderkey=orderkey INT REFERENCES ORDERS 2, 
		 * lineitem.extendedprice=extendedprice DECIMAL  3, 		 
		 * lineitem.discount=discount DECIMAL  4
		 */	
		
		DataRow row = (DataRow) theBinding.entryToObject(dataEntry);
		Datum shipDateDatum = row.getRow().get(1);	// depends on row order from Current Schema of LineItem
		String shipDate = shipDateDatum.getValue().toString();
			 																		
		ByteArrayOutputStream out = new ByteArrayOutputStream(); //not using tuple bind here just for one column!
		DataOutputStream dataOut = new DataOutputStream(out);
		
		try 
		{
			dataOut.writeUTF(shipDate);
			resultEntry.setData(out.toByteArray());
		} 
		catch (IOException willNeverOccur) 
		{
			willNeverOccur.printStackTrace();
		}

		return true;
	}
}
