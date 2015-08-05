/**
 * 
 */
package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.AggregateFunctionColumn;
import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.BL.Evaluator;
import edu.buffalo.cse562.DTO.GroupByOutput;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.Util.Util;
import java.sql.SQLException;
import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;

/**
 * @author Sathish
 *
 */
public class GroupByOperator2 implements Operator {

	private  HashMap<String, ColumnDetail> inputSchema = null;
	private  HashMap<String, ColumnDetail> outputSchema = null;
	private   GroupByOutput groupbyOutput =null;
	private Operator input;
	private List<Column> groupByColumns;
	private List<AggregateFunctionColumn> aggregateFunctions;

	private Operator parentOperator = null;
	String previousValue = "";
	boolean isNew = false;
	boolean isChange = false;
	boolean isGroupBycomputed = false;
	boolean first = true;

	public GroupByOperator2(Operator input, List<Column> groupByColumns,
			List<AggregateFunctionColumn> aggregateFunctions) {
		setChildOp(input);
		this.groupByColumns = groupByColumns;
		this.aggregateFunctions = aggregateFunctions;
		this.outputSchema = getOutputSchema();
		// Util.printSchema(outputSchema);

	}



	@Override
	public ArrayList<Datum> readOneTuple() {

		ArrayList<Datum> inputtuple = null;
		if(isGroupBycomputed) return null;
		GroupByOutput gp = null;

		do
		{
			inputtuple = input.readOneTuple();
			
			if(inputtuple == null) 
			{
				isGroupBycomputed = true;
				ComputeAverage();
				gp = groupbyOutput.clone();
				break;
			}
			
			ArrayList<Datum> gropuByCols = null;

			gropuByCols = getGroupByColumnArrayList(inputtuple, this.groupByColumns);

			String currentValue  = getHashKey(gropuByCols);
			isNew = !currentValue.equals(previousValue) ;
			isChange = isNew && previousValue != "";

			previousValue = currentValue;
			
			if(isChange)
			{
				ComputeAverage();
				gp = groupbyOutput.clone();

			}
			
			int funcIndex = inputtuple.size();
			//System.out.println("previousValue: "+ previousValue + "currentValue: " +currentValue );
			for(AggregateFunctionColumn funcCol:this.aggregateFunctions)
			{
				Evaluator evaluator = new Evaluator(inputtuple,inputSchema);
				Function func = funcCol.getFunction();
				ExpressionList exps = func.getParameters();
				Expression exp;
				if (exps != null){
					exp = (Expression) exps.getExpressions().get(0);
					Datum tup = evaluateExpression(evaluator, exp);
					//System.out.println(isNew);
					handleAggregateFunctions(func,inputtuple,isNew,funcIndex,tup);
					funcIndex++;
				}
				else{
					handleCountFunction(isNew,inputtuple,funcIndex);
					funcIndex++;
				}

			}

			


		}while( !isChange );

		return gp.getOutputData();
	}

	@Override
	public void reset() {
		this.inputSchema = input.getOutputTupleSchema();

	}


	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub

		if(this.outputSchema == null)
		{
			return getOutputSchema();
		}
		return this.outputSchema ;
	}


	private void handleAggregateFunctions(Function func,ArrayList<Datum> outputtuple, boolean isNew, int funcIndex, Datum tup )
	{
		if(func.getName().equalsIgnoreCase("sum"))
		{
			handleSumFunction(isNew,outputtuple,funcIndex, tup);
		}

		if(func.getName().equalsIgnoreCase("avg"))
		{
			handleAvgFunction(isNew,outputtuple,funcIndex,tup);
		}
		if(func.getName().equalsIgnoreCase("min"))
		{
			handleMinFunction(isNew,outputtuple,funcIndex,tup);
		}
		if(func.getName().equalsIgnoreCase("max"))
		{
			handleMaxFunction(isNew,outputtuple,funcIndex,tup);
		}
		if(func.getName().equalsIgnoreCase("count"))
		{
			handleCountFunction(isNew,outputtuple,funcIndex);
		}

	}

	private void handleSumFunction( boolean isNew,  ArrayList<Datum> outputtuple,int funcIndex,Datum tup)
	{

		if(isNew)
		{
			outputtuple.add(tup.cloneTuple(tup));
			groupbyOutput =new GroupByOutput( clone(outputtuple));
		}
		else
		{

			ArrayList<Datum> existingTuple = groupbyOutput.getOutputData();
			if(funcIndex ==existingTuple.size() )
			{
				existingTuple.add(tup.cloneTuple(tup));
			}
			else
			{
				Datum sumDatum = existingTuple.get(funcIndex);
				sumDatum = sumDatum.add(tup.cloneTuple(tup));
			}
		}


	}


	private void handleAvgFunction( boolean isNew,
			ArrayList<Datum> outputtuple, int funcIndex, Datum tup) {

		// average is nothing but sum divided by count
		// so count variable is incremented each time it finds a match
		// since there is no NULL value in text file, we can have a single count for any column
		// if there are two AVG functions, count will be incremented twice for each tuple
		// it's handled in ComputeAverage function 

		if(isNew)
		{
			outputtuple.add(tup.cloneTuple(tup));
			groupbyOutput =new GroupByOutput( outputtuple);
		}
		else
		{


			ArrayList<Datum> existingTuple = groupbyOutput.getOutputData();

			if(funcIndex ==existingTuple.size() )
			{
				existingTuple.add(tup.cloneTuple(tup));
			}
			else
			{
				Datum sumDatum = existingTuple.get(funcIndex);
				sumDatum = sumDatum.add(tup.cloneTuple(tup));
			}
			// System.out.println("AVG "+funcIndex+" " +sumDatum.toString() + " "+existingTuple.get(funcIndex) + " "+  outputData.get(hashKey).getOutputData().get(funcIndex) );
		}

		groupbyOutput.setCount(groupbyOutput.getCount()+1);

	}


	private void handleMinFunction( boolean isNew,
			ArrayList<Datum> outputtuple, int funcIndex, Datum tup) {

		if(isNew)
		{
			outputtuple.add(tup.cloneTuple(tup));
			groupbyOutput =new GroupByOutput( outputtuple);
		}
		else
		{
			ArrayList<Datum> existingTuple = groupbyOutput.getOutputData();


			if(funcIndex ==existingTuple.size() )
			{
				existingTuple.add(tup);
			}
			else
			{
				Datum existingDatum = existingTuple.get(funcIndex);
				existingDatum = (tup.isLessThan(existingDatum))?tup:existingDatum;
				existingTuple.get(funcIndex).Update(existingDatum);
			}
		}


	}

	private void handleMaxFunction( boolean isNew,
			ArrayList<Datum> outputtuple, int funcIndex, Datum tup) {

		if(isNew)
		{
			outputtuple.add(tup.cloneTuple(tup));
			groupbyOutput =new GroupByOutput( outputtuple);
		}
		else
		{
			ArrayList<Datum> existingTuple = groupbyOutput.getOutputData();
			if(funcIndex ==existingTuple.size() )
			{

				existingTuple.add(tup);
			}
			else
			{
				Datum datum = existingTuple.get(funcIndex);
				datum = (tup.isGreaterThan(datum))?tup:datum;
				existingTuple.get(funcIndex).Update(datum);
			}
			//Util.printTuple(existingTuple);
			//Util.printTuple(outputData.get(hashKey).getOutputData());
		}

	}

	private void handleCountFunction( boolean isNew,
			ArrayList<Datum> outputtuple, int funcIndex) {


		Datum tup = new Datum("int","1");
		if(isNew)
		{
			//System.out.println("yay");
			outputtuple.add(tup.cloneTuple(tup));
			groupbyOutput =new GroupByOutput( outputtuple);
		}
		else
		{
			try
			{
				ArrayList<Datum> existingTuple = groupbyOutput.getOutputData();
				if(funcIndex ==existingTuple.size() )
				{
					existingTuple.add(tup);
				}
				else
				{
					Datum sumDatum = existingTuple.get(funcIndex);
					sumDatum = sumDatum.add(tup);
				}
				//System.out.println(hashKey);
				//Util.printTuple(outputData.get(hashKey).getOutputData());
				//System.out.println();
				// Util.printTuple(existingTuple);
				//System.out.println("COUNT "+funcIndex+" " +existingTuple.get(funcIndex) + " "+  groupbyOutput.getOutputData().get(funcIndex) );
			}catch(Exception ex)
			{
				System.out.println("errorrr");
				ex.printStackTrace();
				throw ex;
			}
		}
	}

	private HashMap<String, ColumnDetail> getOutputSchema() {

		copyInputSchemaToOutputSchema();


		//Util.printSchema(outputSchema);
		int index =inputSchema.keySet().size();
		for(AggregateFunctionColumn agf :this.aggregateFunctions)
		{

			String key = agf.getFunction().toString();
			ColumnDetail colDet = getColumnDetailForFunction(agf.getFunction());

			if(colDet.getColumnDefinition() == null)
			{
				System.out.println("colDet.getColumnDefinition() is null");
			}


			colDet.setIndex(index);

			outputSchema.put(key, colDet);

			if(agf.getAliasName()!=null && !agf.getAliasName().equalsIgnoreCase(""))
			{
				outputSchema.put(agf.getAliasName(), colDet.clone());
			}

			index++;
		}
		//Util.printSchema(outputSchema);
		// System.out.println(inputSchema.keySet().size() + " "+this.aggregateFunctions.size() + " " + index);
		return outputSchema;
	}

	private ColumnDetail getColumnDetailForFunction(Function func)
	{

		//colDet.setColumnDefinition(coldef.setColDataType(););
		ColumnDetail colDet = new ColumnDetail();
		colDet.setColumnDefinition(new ColumnDefinition());
		colDet.getColumnDefinition().setColDataType(new ColDataType());
		colDet.getColumnDefinition().getColDataType().setDataType("decimal");
		/*
		ExpressionList exps = func.getParameters();

		if (exps != null){
			for( Object expObj: exps.getExpressions())
			{
				if(expObj instanceof Column)
				{
					colDet = Evaluator.getColumnDetail(outputSchema, (Column) expObj).clone() ;
					if(colDet!=null) return colDet;
				}
			}
		}
		 */
		return colDet;
	}

	private ArrayList<Datum> getGroupByColumnArrayList(ArrayList<Datum> tuple, List<Column> columns )
	{

		ArrayList<Datum> groupByColArrayList = new ArrayList<>();

		if(columns==null||columns.size() ==0 )
			return groupByColArrayList; 

		for(Column col: columns)
		{
			int index =0;
			try
			{
				index = Evaluator.getColumnDetail(inputSchema, col).getIndex() ;
			}
			catch(Exception ex)
			{

				String errorMesage = Util.getSchemaAsString(inputSchema) + "\r\n col: " +col.getWholeColumnName() ; 
				System.out.println(errorMesage);
			}
			groupByColArrayList.add(tuple.get(index));
		}

		return groupByColArrayList;

	}

	private String getHashKey(ArrayList<Datum> groupByColumnTuple)
	{
		if(groupByColumnTuple == null || groupByColumnTuple.size() ==0)
		{
			return "1";
		}
		StringBuilder sb = new StringBuilder();
		for(Datum t:groupByColumnTuple)
		{
			sb.append(t.toString());
			sb.append("|");
		}
		return sb.toString();
	}

	private Datum evaluateExpression(Evaluator evaluator,Expression exp)
	{
		Datum tup =null;
		try {
			tup = new Datum(evaluator.eval(exp));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}

		return tup;
	}

	private List<Integer> getAvgFunctionIndices()
	{
		List<Integer> avgIndices = new ArrayList<Integer>();
		for(Map.Entry<String, ColumnDetail> colDetail: this.outputSchema.entrySet()){

			if(colDetail.getKey().toLowerCase().contains("avg("))
			{
				int index = colDetail.getValue().getIndex();
				// System.out.println("index "+index +" "+ colDetail.getKey());
				avgIndices.add(index);
			}
		}

		return avgIndices;
	}

	private void ComputeAverage()
	{
		List<Integer> avg = getAvgFunctionIndices();
		if(avg.size()>=1)
		{
			// System.out.println("Count"+ colDetail.getValue().getCount());
			Integer count = groupbyOutput.getCount()/avg.size();
			// System.out.println("Count"+ count);
			for(Integer avgIndex :avg)
			{
				// System.out.println("Index:" +avgIndex);
				Datum sum = groupbyOutput.getOutputData().get(avgIndex);
				// System.out.println("Before:"+ colDetail.getValue().getOutputData().get(avgIndex));
				sum = sum.divideBy(new Datum("int",count.toString()));
				// System.out.println("After:"+ colDetail.getValue().getOutputData().get(avgIndex));

			}

		}

	}


	private void copyInputSchemaToOutputSchema()
	{
		outputSchema = new HashMap<String, ColumnDetail>();

		for(Map.Entry<String, ColumnDetail> colDetail: this.inputSchema.entrySet()){
			outputSchema.put(colDetail.getKey(),colDetail.getValue().clone());

		}
	}
	public String toString(){
		return "GROUP BY 2" + groupByColumns ;
	}

	private ArrayList<Datum> clone(ArrayList<Datum> tuple)
	{
		ArrayList<Datum> clonedTuple = new ArrayList<Datum>();

		for( Datum t: tuple)
		{

			clonedTuple.add(t.cloneTuple(t));
		}
		return clonedTuple;
	}



	public void setChildOp(Operator child) {		
		this.input = child;	
		input.setParent(this);
		reset();
	}

	@Override
	public Operator getParent() {
		return this.parentOperator;
	}

	@Override
	public void setParent(Operator parent) {
		this.parentOperator = parent;		
	}

    @Override
    public HashSet<String> getUsedColumns() {
        return null;
    }

    //TODO to sathish : check if u should include aggregate columns in the output of this method getGroupByColumns()
	public List<Column> getGroupByColumns()
	{
		return this.groupByColumns;		
	}


	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return this.input;
	}
}
