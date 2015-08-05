package edu.buffalo.cse562.BDBIndexers;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;
import net.sf.jsqlparser.expression.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class BinderWithFilter extends TupleBinding<DataRow> {
	HashMap<Integer,String> tMap;
    HashSet<Integer> skipSet;

	public BinderWithFilter(HashMap<Integer, String> tMap) {
		super();
		this.tMap = tMap;
	}

	@Override
	public DataRow entryToObject(TupleInput tupleInput) {
		// Get ordered typemap
		ArrayList<Datum> tup = new ArrayList<>();
		for (int i = 0; i<tMap.size(); i++) {
            Datum datum;
			String type = tMap.get(i);
			switch (type) {
				case "int":
				case "INT":
				case "INTEGER":
				case "integer":
				{
					long valueRead = tupleInput.readLong();
                    datum = new Datum(new LongValue(valueRead));
                    tup.add(datum);
					break;
				}
				case "string":
				case "varchar":
				case "char":
				case "STRING":
				case "VARCHAR":
				case "CHAR": {
					String s = tupleInput.readString();
                    tup.add(new Datum(type, s));
					break;
				}
				case "decimal":
				case "DECIMAL": {
                    double d = tupleInput.readDouble();
                    tup.add(new Datum(new DoubleValue(d)));
                    break;
				}
				case "date":
				case "DATE": {
                    String s = tupleInput.readString();
                    tup.add(new Datum("DATE", s));
					break;
				}
				default: {
                    String s = tupleInput.readString();
                    tup.add(new Datum(type, s));
                }
			}

		}
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
				tupleOutput.writeString(col.getValue().toString());
			}
		}
	}
}
