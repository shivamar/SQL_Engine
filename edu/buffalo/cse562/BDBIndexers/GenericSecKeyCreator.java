package edu.buffalo.cse562.BDBIndexers;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.Util.Util;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

public class GenericSecKeyCreator implements com.sleepycat.je.SecondaryKeyCreator {
	private TupleBinding<DataRow> theBinding;
	private int column;
	private String type;

	public GenericSecKeyCreator(TupleBinding<DataRow> secKey, int column, HashMap<Integer, String> typeMap) {
		theBinding = secKey;
		this.column = column;
		this.type = typeMap.get(column);
	}

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb,
            DatabaseEntry keyEntry, 
            DatabaseEntry dataEntry,
            DatabaseEntry resultEntry) {		
		DataRow row = theBinding.entryToObject(dataEntry);
		Datum secIndexDatum = row.getRow().get(column);
		resultEntry.setData(Util.write(secIndexDatum, type));
		return true;
	}
}
