/**
 * 
 */
package edu.buffalo.cse562.Util;

import com.sleepycat.je.DatabaseEntry;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.DTO.ColumnDetail;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


public class Util {

	public static boolean DEBUG = true;

	public static void printSchema(HashMap<String, ColumnDetail> inputSchema)
	{
		if(DEBUG)
		{
			System.out.println("________________________________");
			for(Map.Entry<String, ColumnDetail> colDetail: inputSchema.entrySet()){
				
				try
				{

				System.out.println(colDetail.getKey() + "   " + colDetail.getValue().getIndex() + "  " + colDetail.getValue().getColumnDefinition().getColDataType());
				}
				catch(Exception ex)
				{
					System.out.println("error starts");
					System.out.println("Column name: "+ colDetail.getKey());
					System.out.println("Column index: " + colDetail.getValue().getIndex());
					if(colDetail.getValue().getColumnDefinition() == null)
					{
						System.out.println("colDetail.getValue().getColumnDefinition() is null");
					}
					
					System.out.println("Column ends: "+ colDetail.getValue().getColumnDefinition().getColDataType());
					
					System.out.println("error ends");
					
				}
			}
			System.out.println("________________________________");
		}

	}

	public static String getSchemaAsString(HashMap<String, ColumnDetail> inputSchema)
	{

		StringBuilder str = new StringBuilder();
		for(Map.Entry<String, ColumnDetail> colDetail: inputSchema.entrySet()){

			str.append(colDetail.getKey()) ;
			str.append("|");
		}

		return str.toString();
	}
	public static void printTuple(ArrayList<Datum> singleTuple) {
		if(DEBUG)
		{
			for(int i=0; i < singleTuple.size();i++){


				try
				{
					String str = (singleTuple.get(i)==null)?"":singleTuple.get(i).toString();
					System.out.print(str);
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
					System.out.println(singleTuple.get(i));
				}

				if(i != singleTuple.size() - 1) System.out.print("|");
			}
			System.out.println();
		}
	}
	public static void printToStream(ArrayList<Datum> singleTuple, PrintWriter printStream) {
		StringBuilder b = new StringBuilder();
		for(int i=0; i < singleTuple.size();i++){
			b.append(singleTuple.get(i));
			b.append("|");
		}
		printStream.println(b);
	}

	public static byte[] write(Datum datum, String type){
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		try {
			switch (type) {
				case "int":
				case "INT":
				case "INTEGER":
				case "integer": {
					dataOut.writeLong(((LongValue) datum.getValue()).getValue());
					break;
				}
				case "string":
				case "varchar":
				case "char":
				case "STRING":
				case "VARCHAR":
				case "CHAR": {
					String outString = ((StringValue) datum.getValue()).getValue();
					dataOut.writeUTF(outString);
					break;
				}
				case "decimal":
				case "DECIMAL": {
					dataOut.writeDouble(((DoubleValue) datum.getValue()).getValue());
					break;
				}
				case "date":
				case "DATE": {
					String outString = datum.val.toString();
					dataOut.writeUTF(outString);
				}
				default: {
					dataOut.writeUTF(datum.getValue().toString());
				}
			}
			return out.toByteArray();
		} catch (IOException willNeverOccur)
		{
			willNeverOccur.printStackTrace();
		}
		return null;
	}

    public static HashSet<String> getReferencedColumns(Expression expr){
        HashSet<String> refColumns = new HashSet<>();
        if(expr instanceof BinaryExpression)
        {
            Expression leftExpr = ((BinaryExpression) expr).getLeftExpression();
            Expression rightExpr = ((BinaryExpression) expr).getRightExpression();
            refColumns.addAll(getColumns(leftExpr));
            refColumns.addAll(getColumns(rightExpr));
        }
        else if (expr instanceof Column){
            refColumns.add(((Column) expr).getWholeColumnName());
        }
        return refColumns;
    }

    private static HashSet<String> getColumns(Expression exp){
        HashSet<String> refColumns = new HashSet<>();
        if(exp instanceof Column)
        {
            refColumns.add(((Column) exp).getWholeColumnName());
        }
        else if (exp instanceof Function) {
            ExpressionList explist = ((Function) exp).getParameters();
            for (Object e : explist.getExpressions()) {
                refColumns.addAll(getReferencedColumns((Expression) e));
            }
        }
        return refColumns;
    }

    public static ArrayList<Datum> getTuple(String row, HashMap<Integer, String> indexMaps, HashSet<Integer> skipSet) {
        return null;
    }
}
