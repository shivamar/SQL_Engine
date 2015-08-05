package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Datum;

import java.util.*;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;

//Groups By 
public class SortOperator implements Operator {
	List<OrderByElement> orderByElements;	
	List<Integer> orderByElemIndex = null;
	
	Operator input;
	HashMap<String,ColumnDetail> inputSchema;
	ArrayList<Datum> inputTuple = null;
	ArrayList<Datum> outputTuple = null;
	List<ArrayList<Datum>> fullRelation = new ArrayList<ArrayList<Datum>>();
	boolean isFirstTime;
	int counterForOutputTuple;
	private Operator parentOperator = null;
	
	List<Integer> outputRowsOrder = new ArrayList<Integer>();
	
	public SortOperator(Operator input, List<OrderByElement> orderByElements){
		this.orderByElements = orderByElements;
		setChildOp(input);
		//reset;
	}	

	/* (non-Javadoc)s
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
	public ArrayList<Datum> readOneTuple() {		
		if(	outputTuple == null)  isFirstTime = true;
		else isFirstTime = false;		
		
		//for first time it runs it reads the whole  table into a instance variable called fullRelation as list of list of datums
		if(isFirstTime) {
			inputTuple = input.readOneTuple();
			while (inputTuple != null){			
				fullRelation.add(inputTuple);
				inputTuple = input.readOneTuple();			
			}
			
			List<Integer> inputRowOrder = generateListForEntireTable(fullRelation.size()); 			
			// It prepares the list of output indexes needed and puts it into instance variable 'outputRowsOrder'  
			prepareSortedColumns(0, inputRowOrder);								
		}
		
		//we maintain the counter to keep track of giving the next row from the outputRows eg.its like a line.readNext() 
		if(counterForOutputTuple > outputRowsOrder.size()-1) 
		{
			return null;
		}
		
		int nextIndex = outputRowsOrder.get(counterForOutputTuple);
		outputTuple =  fullRelation.get(nextIndex);
		counterForOutputTuple++;
		return outputTuple;			
	}	
	
	//generates list [1,2,..length-1]
	// list of current indexes[1,2,..length of rows-1] of the full table.
	private List<Integer> generateListForEntireTable(Integer length) {
		List<Integer> list = new ArrayList<Integer>();
		for(int i=0;i < length;i++){
			list.add(i);
		}
		
		return list;
	}
	
	//Input is Original Row Indexes [1,2...nth Row]. List<Integer> rowNumberList
	//The output is a sorted Row Indexes based on Order By elements [33, 55 ,9 , 5...etc] based on order by expressions asc.
	// The output is saved inside List<Integers> outputRowOrder.

	public void prepareSortedColumns(int currIndex, List<Integer> rowNumberList){
		//base case for recursion
		if(currIndex == orderByElemIndex.size()) {
			outputRowsOrder.addAll(rowNumberList);
			return;
		}
				
		int currentOrderByColumnIndex = orderByElemIndex.get(currIndex);
		NavigableMap<Datum, List<Integer>> orderByTreeMap;
		
		// If the nth element is ORDER BY ASC, we use normal tree map If its ORDER BY DESC, we use Treemap.descendingMap()
		if (orderByElements.get(currIndex).isAsc()) 
		{
			orderByTreeMap = new TreeMap<Datum, List<Integer>>();
		}
		 else
		{
			 orderByTreeMap = new TreeMap<Datum, List<Integer>>().descendingMap();
		}
		
		for(Integer rowNum : rowNumberList){
			ArrayList<Datum> row = fullRelation.get(rowNum);
			
			if(orderByTreeMap.containsKey(row.get(currentOrderByColumnIndex))) {
				List<Integer> listOfIndivGroups = orderByTreeMap.get(row.get(currentOrderByColumnIndex));
				
				listOfIndivGroups.add(rowNum);
				orderByTreeMap.put(row.get(currentOrderByColumnIndex), listOfIndivGroups);				
			}				
			else {
				List<Integer> listOfIndivGroups = new ArrayList<Integer>(); listOfIndivGroups.add(rowNum);
				orderByTreeMap.put(row.get(currentOrderByColumnIndex), listOfIndivGroups);
			}			
		}
		
		for(Entry<Datum,List<Integer>> es : orderByTreeMap.entrySet()){
			prepareSortedColumns(currIndex+1, es.getValue());  //recursion call
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() 
	{
		counterForOutputTuple = 0;
		inputSchema =  input.getOutputTupleSchema();
		
		orderByElemIndex = new ArrayList<Integer>();
		
		String orderByElementNameStr = "";
		// get the column index for orderByElements and store it in instance variable orderByElemIndex.
		for(OrderByElement orderByElement : orderByElements)
		{
			orderByElementNameStr = orderByElement.getExpression().toString();
			
			if(orderByElementNameStr != null && orderByElementNameStr.contains("."))
			{
				orderByElemIndex.add(inputSchema.get(orderByElementNameStr).getIndex());
			}
			else
			{
					for(Entry<String,ColumnDetail> es : inputSchema.entrySet())
					{						
						if(es.getKey().contains("."))
						{
						  String[] colNameWithTableName = es.getKey().split("\\."); //<tableName>.<colName>
							
							if(colNameWithTableName[1].equalsIgnoreCase(orderByElementNameStr))
							{
								orderByElemIndex.add(es.getValue().getIndex());
							}
						}
						else
						{  // orderby col is a alias name of a computed aggregate column from query. 
							//select id,sum(orders) AS TOTAL from order group by id order by TOTAL
							if(es.getKey().equalsIgnoreCase(orderByElementNameStr))
							{
								orderByElemIndex.add(es.getValue().getIndex());
							}
						}
					}
			}
		}
	}
	
	
	public String toString(){
		return " ORDER BY " + orderByElements.toString();		
	}
	
	@Override
	public Operator getChildOp() {
		return input;
	}
	
	public void setChildOp(Operator child)
	{
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
        HashSet<String> usedColumns = this.parentOperator.getUsedColumns();
        return null;
    }

    @Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		return inputSchema;
	}

	public List<OrderByElement> setOrderByExpression()
	{
		return this.orderByElements;
	}

	public void setOrderByExpression(List<OrderByElement> orderByElements)
	{
		this.orderByElements = orderByElements;
	}
	public List<OrderByElement> getOrderByColumns()
	{
		return this.orderByElements;		
	}
}
