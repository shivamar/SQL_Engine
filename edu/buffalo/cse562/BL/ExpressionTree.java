package edu.buffalo.cse562.BL;

import edu.buffalo.cse562.DTO.AggregateFunctionColumn;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.Operators.*;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
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
import net.sf.jsqlparser.expression.Function;


public class ExpressionTree {
	public Operator generateTree(SelectBody sel){
		Operator current = null;
		PlainSelect select = (PlainSelect) sel;
		current = addScanOperator(current, select);
		current = addJoinOperator(current, select);
		current = addSelectionOperator(current, select);
		current = addGroupByOperator(current,select);		
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
//			System.out.println("hello "  + table.getName());
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
//			System.out.println("hi" + ((Table) fr).getName());
			current = new CrossProductOperator(new ScanOperator(((Table) fr)), current, j.getOnExpression());
			//current = new JoinOperator(current, new ScanOperator(((Table) fr)), j.getOnExpression());
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
}
