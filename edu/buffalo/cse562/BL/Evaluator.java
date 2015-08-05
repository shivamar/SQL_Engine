/**
 * 
 */
package edu.buffalo.cse562.BL;

import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.DTO.ColumnDetail;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.buffalo.cse562.Operators.IndexScanOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.Util.Util;

public class Evaluator extends Eval {	
	ArrayList<Datum> datum;
	HashMap<String, ColumnDetail> tupleSchema;

	public Evaluator(ArrayList<Datum> datum, HashMap<String, ColumnDetail> tupleSchema)
	{
		this.datum = datum;
		this.tupleSchema = tupleSchema;
	}

	
	@Override
	public LeafValue eval(Column column) {
		
		int colID = getIndex(tupleSchema,column);	
		LeafValue leafValue = null;
		try
		{
			 leafValue = datum.get(colID).val;
		}
		catch(Exception ex)
		{
			System.err.println(datum);
			System.out.println(column.getColumnName());
			System.out.println(colID);
			Util.printSchema(tupleSchema);
		}
		
		// Util.printTuple(tuple);
		return (colID==-1)?null:leafValue;
	}
	
	
	
	
	public static int getIndex(HashMap<String, ColumnDetail> tupleSchema, Column column)
	{
		ColumnDetail col = getColumnDetail(tupleSchema,column);
		return col.getIndex();
	}

	public static ColumnDetail getColumnDetail(HashMap<String, ColumnDetail> tupleSchema, Column column)
	{
		ColumnDetail col = tupleSchema.get(column.getWholeColumnName());
		if(col!=null)
		{
			return col;
		}
		else
		{
			System.err.println("this line should never be hit");
			//Util.printSchema(tupleSchema);
			System.err.println(column.getWholeColumnName());
		
			
			for(Map.Entry<String, ColumnDetail> colDetail: tupleSchema.entrySet()){
				String key = colDetail.getKey();
				if(key.split("\\.").length>1)
				{
					// extract ColumnName from TableName.ColumnName
					String columnValue = key.split("\\.")[1];

					// validate it with the column passed
					if( columnValue.equalsIgnoreCase(column.getWholeColumnName()) || columnValue.equalsIgnoreCase(column.getColumnName()))
					{
						return 	 colDetail.getValue();
					}
				}
			}
		}
		return null;
	}
	
	public static ColumnDetail getColumnDetail(HashMap<String, ColumnDetail> tupleSchema, String columnName)
	{
		ColumnDetail col = tupleSchema.get(columnName);
		if(col!=null)
		{
			return col;
		}
		else
		{
			for(Map.Entry<String, ColumnDetail> colDetail: tupleSchema.entrySet()){
				String key = colDetail.getKey();
				if(key.split("\\.").length>1)
				{
					// extract ColumnName from TableName.ColumnName
					String columnValue = key.split("\\.")[1];

					// validate it with the column passed
					if( columnValue.equalsIgnoreCase(columnName))
					{
						return 	 colDetail.getValue();
					}
				}
			}
		}
		return null;
	}
	
	
}
