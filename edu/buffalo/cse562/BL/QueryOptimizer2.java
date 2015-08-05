package edu.buffalo.cse562.BL;

import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.ExternalSort2;
import edu.buffalo.cse562.Operators.SortOperator;
import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.Operators.SortMergeJoinOperator;
import edu.buffalo.cse562.Operators.SelectionOperator;
import edu.buffalo.cse562.Operators.GroupByOperator2;
import edu.buffalo.cse562.Operators.ExternalSortOperator;
import edu.buffalo.cse562.Operators.CrossProductOperator;
import edu.buffalo.cse562.Operators.GroupByOperator;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class QueryOptimizer2 extends Eval {

	ArrayList<String> tables;
	ArrayList<String> columns;
	ArrayList<String> primarykeys;
	public QueryOptimizer2()
	{

	}

	public QueryOptimizer2(Operator current)
	{
		tables = new ArrayList<String>();
		columns = new ArrayList<String>();
		primarykeys =new ArrayList<String>();
		primarykeys.add("customer.custkey");
		current = pushSelection(current);
		current = replaceOperators(current);		
	}	

	public Operator pushSelection(Operator current)
	{
		Operator currOperator = current;
		Operator parentOperator = current;		

		do{				
			if(currOperator instanceof SelectionOperator)
			{		
				Operator op_modifiedTree = pushDownSelection((SelectionOperator)currOperator);					

				if(op_modifiedTree != null){
					currOperator = op_modifiedTree;
				}
			}	
			parentOperator = currOperator;
			currOperator = parentOperator.getChildOp();
		}
		while(currOperator != null);

		Operator root = getRoot(parentOperator);

		return root;
	}	

	//returns the root of the tree
	private Operator getRoot(Operator currOperator) {		
		while(currOperator.getParent()!=null)
		{
			currOperator = currOperator.getParent();
		}	
		return currOperator;
	}

	/***
	 * Replaces Selection sitting on CrossProducts With Hash Joins
	 * Replaces Group By WIth GroupBy on top of External Sort 
	 * @param root of the unoptimized expression tree
	 * @return root of the new Expression Tree
	 */
	public Operator replaceOperators(Operator current)
	{
		Operator currOperator = current;
		Operator parentOperator = current;		

		do{				
			if(currOperator instanceof SelectionOperator)
			{		
				Operator op_modifiedTree = patternMatchSelectionOnCrossProduct((SelectionOperator)currOperator);					

				if(op_modifiedTree != null){
					currOperator = op_modifiedTree;
				}
			}

			if(currOperator instanceof GroupByOperator)
			{
				GroupByOperator gp =  (GroupByOperator)currOperator;
				List<Column> grpByExpressionsList = gp.getGroupByColumns();

				replaceGroupBy((GroupByOperator)currOperator);		

			}			


			parentOperator = currOperator;
			currOperator = parentOperator.getChildOp();
		}
		while(currOperator != null);

		Operator root = getRoot(parentOperator);

		return root;
	}	

	//iteratively go down the tree checking for order by and chck groupBy below it for same condition matches
	//	private Operator patternMatchSortOnGroupBy(ExternalSortOperator sortOp)
	//	{
	//		Operator oldSortOp = sortOp;				
	//		Operator modifiedSortOp = null;
	//		
	//		do{
	//			if(modifiedSortOp != null) oldSortOp = modifiedSortOp;
	//
	//			modifiedSortOp = replaceSortOnConditionMatch((ExternalSortOperator)oldSortOp);
	//
	//			if(!(modifiedSortOp instanceof ExternalSortOperator)) 
	//			{
	//				return modifiedSortOp;
	//			}
	//
	//		}while(!modifiedSortOp.equals(oldSortOp));
	//		
	//		return modifiedSortOp;

	//		return replaceSortOnConditionMatch(sortOp);
	//	}

	/***
	 *check all groupBy under a sort until condition matches or if child is null
	if a match is found replace the underlying grp by with sorted groupBy2
	delete the present sortOp and send the parent operator reference of sortOp
	else send back the operator
	 * @param sortOp
	 * @return
	 */

	private Operator replaceSortOnConditionMatch(ExternalSortOperator sortOp)
	{
		if(sortOp == null || sortOp.getChildOp() == null) return null;

		Set<String> hashSet_OrderBy = getHashSet(sortOp.getOrderByColumns());
		Operator childOp = sortOp.getChildOp();

		while(childOp != null)
		{
			if(childOp instanceof GroupByOperator)
			{
				GroupByOperator grpBy = ((GroupByOperator) childOp);
				if(checkSortGroupConditionMatch(hashSet_OrderBy, grpBy.getGroupByColumns()))
				{
					Operator modifiedTree = replaceGroupBy(grpBy);

					//Delete current sort operator and give back the parentOperatr reference(to iteration purpose) to the caller function
					sortOp.getParent().setChildOp(modifiedTree);					
					return sortOp.getParent();
				}
			}

			childOp = childOp.getChildOp();
		}

		return sortOp;
	}

	private Set<String> getHashSet(List<OrderByElement> orderByCols)
	{
		Set<String> hashSet = new HashSet<String>();

		for(OrderByElement orderByElem : orderByCols)
		{
			hashSet.add(orderByElem.getExpression().toString());
		}

		return hashSet;
	}

	//check whether the conditions of order by and group by.
	private boolean checkSortGroupConditionMatch(Set<String> hashSet_OrderBy, List<Column> groupByCols)
	{
		if(hashSet_OrderBy.size() != groupByCols.size()) return false;

		for(Expression expr : groupByCols)
		{
			if(!hashSet_OrderBy.contains(expr.toString())) return false;
		}

		return true;
	}

	//iteratively keep sending the input selection down to pattern match cross products sitting below
	//until select conditions are empty or until the last child in the tree
	private Operator patternMatchSelectionOnCrossProduct(SelectionOperator selectionOperator)
	{
		Operator oldSelectOp = selectionOperator;				
		Operator modifiedSelect = null;		

		do{
			if(modifiedSelect != null) oldSelectOp = modifiedSelect;

			modifiedSelect = replaceCrossProductWithSortMerge((SelectionOperator)oldSelectOp);

			if(!(modifiedSelect instanceof SelectionOperator)) 
			{
				return modifiedSelect;
			}

		}while(!modifiedSelect.equals(oldSelectOp));

		return modifiedSelect;
	}

	//pushes Selection below cross product wherever applicable
	private Operator pushDownSelection(SelectionOperator selectionOperator)
	{
		if(selectionOperator == null || selectionOperator.getChildOp() == null) return null;

		List<Expression> exprList = splitANDClauses(selectionOperator.getExpression());
		Operator op = selectionOperator;

		do{
			if(op.getChildOp() instanceof CrossProductOperator)
			{
				CrossProductOperator joinOp = (CrossProductOperator) op.getChildOp();

				List<Expression> newSelLeftExpr_List = new LinkedList<Expression>(); 
				List<Expression> newSelRightExpr_List = new LinkedList<Expression>(); 

				populateNewSelectionList(newSelLeftExpr_List, newSelRightExpr_List, exprList, joinOp);

				if(newSelLeftExpr_List.size() > 0) 
				{
					Operator oper = new SelectionOperator(joinOp.getLeftOperator(), newSelLeftExpr_List);

					//left deep tree. left operator is the child! 
					joinOp.setChildOp(oper);
				}
				if(newSelRightExpr_List.size() > 0)
				{
					Operator oper = new SelectionOperator(joinOp.getRightOperator(), newSelRightExpr_List);

					joinOp.setRightOp(oper);
				}

				// if the selection has any conditions to enforce re-initialise it with modified expression list
				// if it does not have any conditions to enforce just rest the parent child relation of its parent
				if(exprList.size() > 0) 
				{
					selectionOperator.setSelectExpression(exprList);
					return selectionOperator;
				}
				else 
				{
					selectionOperator.getParent().setChildOp(selectionOperator.getChildOp());

					return selectionOperator.getParent();
				}
			}

			op = op.getChildOp();
		}while(op.getChildOp()!= null);

		return selectionOperator;
	}


	private void populateNewSelectionList(List<Expression> newSelLeftExpr_List, List<Expression> newSelRightExpr_List, List<Expression> selectionExprList, CrossProductOperator joinOp)
	{		
		for(Iterator<Expression> itr =  selectionExprList.iterator(); itr.hasNext();)
		{
			Expression expr = itr.next();	

			if(checkIfExpressionIsPushable(expr, joinOp, newSelLeftExpr_List, newSelRightExpr_List)) itr.remove();			
		}
	}

	private boolean checkIfExpressionIsPushable(Expression expr, CrossProductOperator joinOp, List<Expression> newSelLeftExpr_List, List<Expression> newSelRightExpr_List)
	{
		boolean isExistsInLeft = false;
		boolean isExistsInRight = false;

		if(expr instanceof BinaryExpression) 
		{
			//if both the left n right expr is a column and the table names are diff return false//TODO
			Expression leftExpr = ((BinaryExpression) expr).getLeftExpression();
			Expression rightExpr = ((BinaryExpression) expr).getRightExpression();

			if(checkIfExprRefersMoreThanOneRelation(leftExpr, rightExpr)) return false;

			if(leftExpr instanceof Column)
			{
				if(expressionExistsInSchema(((Column) leftExpr).getWholeColumnName(), joinOp.getLeftOperator().getOutputTupleSchema().keySet()))
					isExistsInLeft = true;
				if(expressionExistsInSchema(((Column) leftExpr).getWholeColumnName(), joinOp.getRightOperator().getOutputTupleSchema().keySet()))
					isExistsInRight = true;
			}

			if(rightExpr instanceof Column)
			{
				if(expressionExistsInSchema(((Column) rightExpr).getWholeColumnName(), joinOp.getLeftOperator().getOutputTupleSchema().keySet()))
					isExistsInLeft = true;
				if(expressionExistsInSchema(((Column) rightExpr).getWholeColumnName(), joinOp.getRightOperator().getOutputTupleSchema().keySet()))
					isExistsInRight = true;
			}
		}
		else 
		{

			tables.clear();
			evaluateExpression(expr);
			if(tables.size() ==1)
			{
				String colName = columns.get(0);
				if(expressionExistsInSchema(colName, joinOp.getLeftOperator().getOutputTupleSchema().keySet()))
					isExistsInLeft = true;
				if(expressionExistsInSchema(colName, joinOp.getRightOperator().getOutputTupleSchema().keySet()))
					isExistsInRight = true;

			}
			else
			{
				return false;
			}
			// System.out.println(expr);
		}

		if(isExistsInLeft && isExistsInRight) 
		{
			// System.out.println("both left and right");
			return false;
		}
		else if((isExistsInLeft && !(isExistsInRight)))
		{										
			newSelLeftExpr_List.add(expr);
			return true;
		}
		else if((isExistsInRight && !(isExistsInLeft)))
		{
			newSelRightExpr_List.add(expr);
			return true;
		}

		return false;
	}

	//returns true if expression refers more than a relation
	private boolean checkIfExprRefersMoreThanOneRelation(Expression leftExpr,Expression rightExpr)
	{
		if((leftExpr instanceof Column) && (rightExpr instanceof Column))
		{
			String leftTable = ((Column) leftExpr).getTable().getName();
			String rightTable = ((Column) rightExpr).getTable().getName();

			if(!(leftTable.equalsIgnoreCase(rightTable))) return true;				
		}

		return false;
	}
	/***
	 * conversion of cross prod to joins defined only on equi joins now in project 2
	 * @param selectionOperator
	 * @return
	 */
	private Operator replaceCrossProductWithSortMerge(SelectionOperator selectionOperator)
	{
		if(selectionOperator == null || selectionOperator.getChildOp() == null) return null;

		List<Expression> exprList = splitANDClauses(selectionOperator.getExpression());
		Operator childOperator = selectionOperator.getChildOp();

		do
		{			
			if(childOperator instanceof CrossProductOperator)
			{
				CrossProductOperator crossPOperator = (CrossProductOperator)childOperator;						

				for(Iterator<Expression> itr =  exprList.iterator(); itr.hasNext();)
				{
					Expression expr = itr.next();

					if(expr instanceof EqualsTo)
					{
						EqualsTo equalsExpr = (EqualsTo)expr;

						if(expressionMatchesJoinOp(equalsExpr, crossPOperator))
						{
							Operator leftExtSort;
							Operator rightExtSort;

							if(expressionExistsInSchema(equalsExpr.getLeftExpression(), crossPOperator.getLeftOperator().getOutputTupleSchema().keySet()))
							{
								leftExtSort = new ExternalSort2(crossPOperator.getLeftOperator(), getOrderByElemList(equalsExpr.getLeftExpression()));
								rightExtSort = new ExternalSort2(crossPOperator.getRightOperator(), getOrderByElemList(equalsExpr.getRightExpression()));
							}
							else
							{
								leftExtSort = new ExternalSort2(crossPOperator.getLeftOperator(), getOrderByElemList(equalsExpr.getRightExpression()));
								rightExtSort = new ExternalSort2(crossPOperator.getRightOperator(), getOrderByElemList(equalsExpr.getLeftExpression()));
					
							}							

							SortMergeJoinOperator sortMergeJoinOp = new SortMergeJoinOperator(leftExtSort, rightExtSort, equalsExpr);

							itr.remove();
							childOperator.getParent().setChildOp(sortMergeJoinOp);	
							break;
						}
					}											
				}	
				//selectionOperator.setSelectExpression(exprList); //TODO
				// if the selection has any conditions to enforce re-initialise it with modified expression list
				// if it does not have any conditions to enforce just rest the parent child relation of its parent
				if(exprList.size() > 0) 
				{
					selectionOperator.setSelectExpression(exprList);
				}
				else 
				{
					selectionOperator.getParent().setChildOp(selectionOperator.getChildOp());

					return selectionOperator.getParent();
				}
			}	
			childOperator = childOperator.getChildOp();
		} while(childOperator != null);

		return selectionOperator;
	}

	/***
	 *  checks if expression matches with a join operator
	 */
	private Boolean expressionMatchesJoinOp(EqualsTo equalsExpression, CrossProductOperator crossPop)
	{		
		HashMap<String, ColumnDetail> leftSchema = (crossPop.getLeftOperator()).getOutputTupleSchema();
		HashMap<String, ColumnDetail> rightSchema = (crossPop.getRightOperator()).getOutputTupleSchema();

		Expression left =  ((EqualsTo) equalsExpression).getLeftExpression();
		Expression right =  ((EqualsTo) equalsExpression).getRightExpression();

		if(left instanceof Column && right instanceof Column)
		{
			String leftExprName_Str = ((Column) left).getWholeColumnName();
			String rightExprName_Str = ((Column) right).getWholeColumnName();

			if(
					(
							expressionExistsInSchema(leftExprName_Str, leftSchema.keySet()) && 
							expressionExistsInSchema(rightExprName_Str, rightSchema.keySet())
							)
							|| 
							(		
									expressionExistsInSchema(leftExprName_Str, rightSchema.keySet()) && 
									expressionExistsInSchema(rightExprName_Str, leftSchema.keySet())
									)
					)
			{
				return true;
			}					
		}

		return false;
	}

	/***
	 *  checks if column exists in Schema
	 */
	private boolean expressionExistsInSchema(String colName, Set<String> keySet) {
		for(String key : keySet)
		{
			if(colName.equalsIgnoreCase(key)) return true;
		}
		return false;
	}

	/***
	 *  checks if column exists in Schema
	 */
	private boolean expressionExistsInSchema(Expression expr, Set<String> keySet) {
		String colName = ((Column) expr).getWholeColumnName();

		for(String key : keySet)
		{
			if(colName.equalsIgnoreCase(key)) return true;
		}
		return false;
	}

	/***
	 * Splits and clauses to multiple and clauses
	 * @param e
	 * @return
	 */
	private List<Expression> splitANDClauses(Expression e) {
		List<Expression> ret = new LinkedList<Expression>();

		if(e instanceof AndExpression){
			AndExpression a = (AndExpression)e;
			ret.addAll(
					splitANDClauses(a.getLeftExpression())
					);
			ret.addAll(
					splitANDClauses(a.getRightExpression())
					);
		} else {
			ret.add(e);
		}

		return ret;
	}








	//replaceGroupBy to Sorted Group By
	private Operator replaceGroupBy(GroupByOperator groupByOp)
	{
		boolean isOptimised = false;
		//System.out.println("good");
		List<Column> grpByExpressionsList = groupByOp.getGroupByColumns();
		List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
		//System.out.println(grpByExpressionsList.size());
		Column column = groupByOp.getGroupByColumns().get(0);
		String columnName = column.getWholeColumnName();
		isOptimised = primarykeys.contains(columnName);
		//System.out.println(isOptimised);
		//System.out.println(columnName);
		for(Expression exp : grpByExpressionsList)
		{
			OrderByElement orderByElem = new OrderByElement();
			if(exp instanceof Column)
			{
				
				if(((Column)exp).getWholeColumnName().equalsIgnoreCase(columnName))
				{
					orderByElem.setExpression(exp);
					orderByElements.add(orderByElem);	
				}
				else
				{
					//System.out.println(((Column)exp).getColumnName());
				}
			}
			else
			{
				//System.out.println(exp);
			}

		}
		ExternalSortOperator SortOp = new ExternalSortOperator(groupByOp.getChildOp(), orderByElements);

		// optimisation:


		GroupByOperator2 groupBy2 = null;
		List<Column> groupByColumn = new ArrayList<Column>();
		if(isOptimised)
		{
			System.out.println("good");
			groupByColumn.add(column);
			groupBy2 = new GroupByOperator2(SortOp, 
					groupByColumn
					, 
					groupByOp.getAggregateFunctions());
		}
		else
		{
			System.out.println("bad");
			groupByColumn.add(groupByOp.getGroupByColumns().get(0));
			groupBy2 = new GroupByOperator2(SortOp, 
					groupByOp.getGroupByColumns(), 
					groupByOp.getAggregateFunctions());
		}
		//System.out.println("settng child");
		groupByOp.getParent().setChildOp(groupBy2);		
		//System.out.println("child set ");

		return groupBy2;
	}


	//returns a list of order by elem for purpose of external sort
	public List<OrderByElement> getOrderByElemList(Expression expr)
	{
		List<OrderByElement> orderByElemList = new ArrayList<OrderByElement>();
		OrderByElement orderByElem = new OrderByElement();
		orderByElem.setExpression(expr);
		orderByElemList.add(orderByElem);
		return orderByElemList;
	}

	@Override
	public LeafValue eval(Column column) throws SQLException {
		// TODO Auto-generated method stub
		String tablename = column.getTable().getName();
		if(!tables.contains(tablename))
			tables.add(tablename);

		columns.add(column.getWholeColumnName());
		return null;
	}

	private void evaluateExpression(Expression exp)
	{
		try {
			eval(exp);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}

	}
}
