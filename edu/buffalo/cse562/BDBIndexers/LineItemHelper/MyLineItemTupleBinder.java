package edu.buffalo.cse562.BDBIndexers.LineItemHelper;

import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;

public class MyLineItemTupleBinder extends TupleBinding<DataRow> {		
	
	@Override
	public DataRow entryToObject(TupleInput tupleInput) {
		/*  Current Schema LineItem!!
		 * lineitem.linenumber=linenumber INT  0, 
		 * lineitem.shipdate=shipdate DATE  1, 
		 * lineitem.orderkey=orderkey INT REFERENCES ORDERS 2, 
		 * lineitem.extendedprice=extendedprice DECIMAL  3, 		 
		 * lineitem.discount=discount DECIMAL  4
		 */						
		ArrayList<Datum> tup = new ArrayList<Datum>();
		
		tup.add(new Datum(new LongValue(tupleInput.readLong())));
		tup.add(new Datum("DATE",tupleInput.readString())); 	//DATE		
		tup.add(new Datum(new LongValue(tupleInput.readLong())));
		tup.add(new Datum(new DoubleValue(tupleInput.readDouble())));
		tup.add(new Datum(new DoubleValue(tupleInput.readDouble())));

		return new DataRow(tup);
	}

	@Override
	public void objectToEntry(DataRow row, TupleOutput tupleOutput) 
	{
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
			}						
		}				
	}
}
