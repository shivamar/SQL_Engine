package edu.buffalo.cse562.BL;

import edu.buffalo.cse562.Operators.*;
import edu.buffalo.cse562.DTO.AggregateFunctionColumn;
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.Util.Util;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.HashMap;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SanitizeQuery extends Eval {

	HashMap<String,String> tableNameAlias = null;

	HashMap<String,Integer> joinTables;
	HashMap<String, String> primaryKeys;
	public SanitizeQuery()
	{
		joinTables = new  HashMap<String,Integer>();
		primaryKeys = new HashMap<String, String>();
		setJoinTableOrders();
		setPrimaryKeys();
	}




	public Operator generateTree(SelectBody sel){
		Operator current = null;

		PlainSelect select = (PlainSelect) sel;	

		tableNameAlias = getAllTableNames(select);
		evaluateGroupByColumns(select);
		evaluateOrderBy(select);
		evaluateSelect(select);
		evaluateExtendedProjection(select);

		current = addScanOperator(current, select);
		current = addJoinOperator(current, select);
		current = addSelectionOperator(current, select);
		current = addGroupByOperator(current, select);
		current = addSortOperator(current, select);
		current = addExtendedProjectionOperator(current, select);
		current = addLimitOperator(current, select);
		return current;
	}

	private Operator addSortOperator(Operator current, PlainSelect select){
		List<OrderByElement> OrderByElements  = (List<OrderByElement>)select.getOrderByElements();
		if(OrderByElements != null && OrderByElements.size() > 0){
			current = new ExternalSortOperator(current, OrderByElements);
		}

		return current;
	}
	//
	private Operator addJoinOperator(Operator current,PlainSelect select)
	{ 
  
		List<Join> joins = select.getJoins();
		if (joins != null){


			if (joins.size() > 0){
				for (Join j : joins){
					current = buildJoins(current, j);
				}
			}
		}

		return current;
	}

	private Operator addSelectionOperator(Operator current,PlainSelect select)
	{
		Expression exp = (Expression) select.getWhere();
		if (exp != null){
			current = new SelectionOperator(current, exp);
		}
		return current;
	}

	private Operator addExtendedProjectionOperator(Operator current,PlainSelect select)
	{
		List<SelectItem> selItems = (List<SelectItem>) select.getSelectItems();
		if (selItems != null){
			if (selItems.size() > 0){				
				current = new ExtendedProjection(current, selItems);
			}
		}
		return current;
	}
	private Operator addScanOperator(Operator current,PlainSelect select)
	{
		FromItem fi = select.getFromItem();
		Table table  = null;		
		if (fi instanceof Table){
			table = (Table) fi;
			Expression exp = (Expression) select.getWhere();
//			current = new IndexScanOperator(table,exp);
			current = new ScanOperator(table);
		}
		else if (fi instanceof SubSelect){
			current = generateTree(((SubSelect) fi).getSelectBody());
		}		
		else if (fi instanceof SubJoin){
		}
		return current;
	}
	public Operator buildJoins(Operator current, Join j){
		FromItem fr = j.getRightItem();

		if (fr instanceof Table){
			Expression exp = j.getOnExpression();
			evaluateExpression(exp);
			current = new CrossProductOperator(current, new ScanOperator(((Table) fr)), j.getOnExpression());
		}

		return current;
	}

	public Operator addLimitOperator(Operator current, PlainSelect select){
		Limit lim = (Limit) select.getLimit();		
		if (lim != null){
			return new LimitOperator(current, lim);
		}
		return current;
	}

	private Operator addGroupByOperator(Operator current,PlainSelect select)
	{


		List<Column> groupByColumns =  getGroupByColumns(select);
		List<AggregateFunctionColumn> aggregateFunctions = getFunctionList( select);

		if(groupByColumns!=null ||aggregateFunctions.size() >0 )
		{
			current = new GroupByOperator(current, groupByColumns,aggregateFunctions );
		}

		// if group by has a 'having' condition add a select operator
		Expression exp = (Expression) select.getHaving();
		if (exp != null){
			current = new SelectionOperator(current, exp);
		}
		return current;
	}

	private void evaluateGroupByColumns(PlainSelect select)
	{
		List<Expression> groupByColumnExp = (List<Expression>) select.getGroupByColumnReferences();
		if(groupByColumnExp == null ) return; 
		for(Expression exp: groupByColumnExp)
		{
			evaluateExpression(exp);
		}
	}
	private List<Column> getGroupByColumns(PlainSelect select)
	{
		List<Expression> groupByColumnExp = (List<Expression>) select.getGroupByColumnReferences();
		List<Column> groupByColumns = null;
		if(groupByColumnExp!=null)
		{
			groupByColumns = new ArrayList<Column>();

			for(Expression exp: groupByColumnExp)
			{

				if( exp instanceof Column)
				{

					groupByColumns.add((Column)exp);

				}
			}
		}
		return groupByColumns;
	}

	private List<AggregateFunctionColumn> getFunctionList(PlainSelect select)
	{
		List<SelectItem> selItems = (List<SelectItem>) select.getSelectItems();
		List<AggregateFunctionColumn> functionList = new ArrayList<AggregateFunctionColumn>();

		for(SelectItem selItem:selItems )
		{
			if(selItem instanceof SelectExpressionItem)
			{

				String alias = ((SelectExpressionItem)selItem).getAlias();
				Expression expr = ((SelectExpressionItem) selItem).getExpression();
				if(expr instanceof Function)
				{	
					AggregateFunctionColumn agfc = new AggregateFunctionColumn();
					agfc.setFunction((Function)expr);
					agfc.setAliasName(alias);
					functionList.add(agfc);
				}}		
		}
		return functionList;
	}

	@Override
	public LeafValue eval(Column column) throws SQLException {
		// TODO Auto-generated method stub
		try
		{
			String tableName = getTableName(column.getColumnName().toLowerCase());
			// System.out.println(" tablename "+ tableName);
			// System.out.println("getWholeColumnName: "+ column.getWholeColumnName());
			if(tableName !=null && column.getColumnName()==column.getWholeColumnName())
			{
				//System.out.println("Setting table");
				tableName =tableName.toLowerCase();
				Table t = new Table();
				t.setName(tableName);
				column.setTable(t);
			}
			if(column.getTable() !=null)
			{
				if(column.getTable().getName() !=null)
				{
					column.getTable().setName(column.getTable().getName().toLowerCase());
				}
			}

			column.setColumnName(column.getColumnName().toLowerCase());

			String tn = column.getWholeColumnName().split("\\.")[0];
			addColumnToTable(tn, column.getColumnName());

		}
		catch(Exception ex)
		{
			System.err.println("Error while sanitising the column: " + column.getColumnName());
			ex.printStackTrace();
		}
		return null;
	}


	public HashMap<String,String> getAllTableNames(PlainSelect select)
	{
		HashMap<String,String> tableNameAliasMap = new HashMap<String, String>();
		FromItem fi = select.getFromItem();
		Table table  = null;		
		if (fi instanceof Table){
			Table t = ((Table) fi);
			tableNameAliasMap.put(t.getName(), t.getAlias());

		}

		List<Join> joins = select.getJoins();
		if (joins != null){
			if (joins.size() > 0){
				for (Join j : joins){
					FromItem fr = j.getRightItem();
					if(fr instanceof Table)
					{
						Table t = ((Table) fr);
						tableNameAliasMap.put(t.getName(), t.getAlias());
					}

				}
			}
		}
		return tableNameAliasMap;
	}

	private String getTableName(String columnName)
	{

		String key = "";
		String tableName = "";
		try
		{
			for(Map.Entry<String,String> tabNameAlias:tableNameAlias.entrySet())
			{
				tableName = tabNameAlias.getKey().toLowerCase();
				key = tableName + "." + columnName.toLowerCase();
				if( Main.tableMapping.get(tableName).get(key)!=null) 
				{

					return tabNameAlias.getKey();
				}
			}
		}catch( Exception ex)
		{
			System.err.println(columnName);
			System.err.println(key);

			Util.printSchema(Main.tableMapping.get(tableName));


			throw ex;

		}
		return null;

	}

	private void evaluateExpression(Expression exp)
	{
		if(exp!=null)
		{
			try {
				eval(exp);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
		}
	}

	private void evaluateOrderBy(PlainSelect select)
	{
		List<OrderByElement> OrderByElements  = (List<OrderByElement>)select.getOrderByElements();		
		if(OrderByElements == null) return;
		for(OrderByElement o : OrderByElements)
		{
			evaluateExpression(o.getExpression());
		}

	}
	private void evaluateSelect(PlainSelect select)
	{
		Expression exp = (Expression) select.getWhere();
		if ( exp !=null)
		{
			evaluateExpression(exp);
		}

	}

	private void evaluateExtendedProjection(PlainSelect select)
	{
		List<SelectItem> selItems = (List<SelectItem>) select.getSelectItems();
		for(SelectItem s : selItems)
		{
			if(!(s instanceof AllTableColumns) && !(s instanceof AllColumns))
			{
				Expression expr = ((SelectExpressionItem) s).getExpression();
				if(!(expr instanceof Function))
				{
					//System.out.println("evaluating: " +expr );

					evaluateExpression(expr);
				}
				else if( expr!=null)
				{
					try
					{
						ExpressionList exps =((Function)expr).getParameters();
						if(exps!=null)
						{
							Expression exp = (Expression) exps.getExpressions().get(0);
							evaluateExpression(exp);
						}
					}
					catch (Exception ex)
					{
						// System.err.println("func evaluating: " +expr );
						// ex.printStackTrace();
					}

					//System.out.println("func evaluating: " +expr );
				}}

		}

	}


	public void addColumnToTable(String tableName, String columnName)
	{
		// System.out.println("tableName: "+ tableName + "   columnName: " + columnName);
		ArrayList<String> columns = null;

		if(tableName!=null &&columnName !=null )
		{
			columns = Main.tableColumns.get(tableName);
			if(columns == null)
			{
				columns = new ArrayList<String>();

				Main.tableColumns.put(tableName, columns);
			}

			if(!columns.contains(columnName))
			{
				columns.add(columnName);
			}

		}
	}

	private List<Join>  getNewJoinOrder(List<Join> joins )
	{
		List<Join> newJoins = new ArrayList<Join>();
		Join[] newJoinArr = new Join[6];

		//may not be necessary but dont know about java


		for(Join j : joins)
		{
			if(j.getRightItem() instanceof Table)
			{
				Table t = (Table)j.getRightItem();

				int index = joinTables.get(t.getName());
				newJoinArr[index] = j;
			}

		}

		for( int i = 0; i <newJoinArr.length ; i++)
		{
			if(newJoinArr[i] != null)
			{
				newJoins.add(newJoinArr[i]);
			}
		}

		return newJoins;
	}
	private void setJoinTableOrders()
	{
		joinTables.put( "region",0);
		joinTables.put( "nation",1);
		joinTables.put( "supplier",2);
		joinTables.put("customer",3);
		joinTables.put("orders",4);
		joinTables.put("lineitem",5);

	}

	private void setPrimaryKeys() {
		primaryKeys.put("customer", "custkey");
	}
}
