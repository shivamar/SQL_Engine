/**
 * 
 */
package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.BL.Evaluator;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Datum;
import java.sql.SQLException;
import java.util.*;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

/**
 * @author Sathish
 *
 */
public class SelectionOperator implements Operator {

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */	
	Operator input;
	Expression exp;
	private  HashMap<String, ColumnDetail> inputSchema = null;
	Operator parentOperator = null;

	public SelectionOperator(Operator input, Expression exp){
		setChildOp(input);		
		setSelectExpression(exp);
		//Util.printSchema(inputSchema);
	}
	
	//TODO change
	public SelectionOperator(Operator input, List<Expression> expList){
		setChildOp(input);		
		setSelectExpression(expList);
				
		//Util.printSchema(inputSchema);
	}
	
	public ArrayList<Datum> readOneTuple() {
		
		ArrayList<Datum> tuple = null;
		boolean result =  false;
		do
		{
			tuple = input.readOneTuple();
			
			if(tuple==null){
                return null;
            }
			// Util.printTuple(tuple);
			Evaluator evaluator = new Evaluator(tuple,inputSchema);
			
			try {
				
				BooleanValue bv= (BooleanValue) evaluator.eval(exp);
				result = bv.getValue();
				if(result)
				{
                    return tuple;
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("exp has thrwn exp"+exp.toString());
				e.printStackTrace();
			}
			catch (Exception ex)
			{
				System.out.println("exp has thrwn exp  "+exp.toString());
				ex.printStackTrace();
				throw ex;
			}
		}while(!result);
		return tuple;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	public void reset() {
		this.inputSchema = input.getOutputTupleSchema();	
	}
	
	/**
	 * forms AND Expression and sets the select operators exp
	 * @param expList List of Expressions which should be conjugated
	 */
	public void formANDExpression(List<Expression> expList){				
		if(expList == null) return;
			
		if(expList.size() == 1) {
			Iterator<Expression> itr = expList.iterator();
			this.exp = itr.next();
		} 
		else {
			Iterator<Expression> itr = expList.iterator();
			Expression andExp =  new AndExpression(itr.next(), itr.next());
			while(itr.hasNext())
			{
				andExp = new AndExpression(andExp, itr.next());
			}
			this.exp = andExp;
		}		
	}
	
	public Expression getSelectExpression()
	{
		return this.exp;
	}

	public void setSelectExpression(List<Expression> expList)
	{
		formANDExpression(expList);
	}

	public void setSelectExpression(Expression expr)
	{
		this.exp = expr;
	}
	
	public String toString(){
		
		return " Selection Operator:  "+ this.exp;
	}
	
	public Operator getChildOp(){
		return this.input;
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
        return null;
    }

    public Expression getExpression()
	{
		return this.exp;
	}
	
	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {		
		return this.inputSchema;
	}
}
