package edu.buffalo.cse562.BDBIndexers.OrdersHelper;

import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;

public class MyOrdersTupleBinder extends TupleBinding<DataRow> {
	/*   orders.orderdate=orderdate DATE  0, 
		 orders.orderkey=orderkey INT  1, 
		 orders.custkey=custkey INT REFERENCES CUSTOMER 2,
		 orders.shippriority=shippriority INT  3}
	 */ 
	@Override
	public DataRow entryToObject(TupleInput tupleInput) {
		ArrayList<Datum> tup = new ArrayList<Datum>();

		//tup.add(new Datum("DATE",convertToTimeString(tupleInput.readLong()))); 	//DATE
		tup.add(new Datum("DATE",tupleInput.readString()));
		tup.add(new Datum(new LongValue(tupleInput.readLong())));
		tup.add(new Datum(new LongValue(tupleInput.readLong())));
		tup.add(new Datum(new LongValue(tupleInput.readLong())));

		return new DataRow(tup);
	}

	@Override
	public void objectToEntry(DataRow row, TupleOutput tupleOutput) {
		 // Write the data to the TupleOutput (a DatabaseEntry).
        // Order is important. The first data written will be
        // the first bytes used by the default comparison routines.
		
		ArrayList<Datum> tuple = row.getRow();
		
		for(Datum col : tuple){
			LeafValue colVal = col.val;
			
			if(colVal instanceof StringValue)
			{			
				tupleOutput.writeString(((StringValue) colVal).getValue());
			}		
			
			else if(colVal instanceof DoubleValue)
			{			
				tupleOutput.writeDouble(((DoubleValue) colVal).getValue());
			}
			else if(colVal instanceof LongValue)
			{
				tupleOutput.writeLong(((LongValue) colVal).getValue());
			}
			else if(colVal instanceof DateValue)
			{
				//No writeDate available!..we should try string comparisons! TO CHECK
				tupleOutput.writeString(colVal.toString());
//				SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
//				
//				try 
//				{
//					Long timestamp = parser.parse(colVal.toString()).getTime();
//					tupleOutput.writeLong(timestamp);									
//				} 
//				catch (ParseException e) 
//				{
//					e.printStackTrace();
//				} 
				
				
			}						
		}		
	}
	
	public String convertToTimeString(long time){
	    Date date = new Date(time);
	    Format format = new SimpleDateFormat("yyyy-MM-dd");
	    return format.format(date);
	}

}
