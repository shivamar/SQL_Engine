/**
 * 
 */
package edu.buffalo.cse562.Operators;
import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Datum;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;

/**
 * @author Sathish
 *
 */
public class CrossProductOperator implements Operator {
	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	//TODO: Create setters and getters
	private Operator left;
	private Operator right;
	private HashMap<String, ColumnDetail> outputSchema = null;
	private HashMap<String, ColumnDetail> leftSchema;
	private HashMap<String, ColumnDetail> rightSchema;
	private Expression expr = null;
	private ArrayList<Datum> leftTuple;
	private ArrayList<Datum> rightTuple;
	private ArrayList<Datum> outputTuple;
	private int outputSize;

	private Operator parentOperator = null;
	

	public CrossProductOperator(Operator left, Operator right,	Expression expr){
		setChildOp(left);
		
		setRightOp(right);
		
		this.expr = expr;
		generateOutputSchema();
	}
	
	@Override
	public ArrayList<Datum> readOneTuple() {
		if(this.leftTuple == null) this.leftTuple = left.readOneTuple();
		rightTuple = right.readOneTuple();		

		if (rightTuple == null){
			right.reset();
			rightTuple = right.readOneTuple();
			this.leftTuple = left.readOneTuple();
		}
		
		if (leftTuple == null || rightTuple == null){
			return null;
		}
		
		outputTuple = new ArrayList<Datum>(outputSize);
		outputTuple.addAll(leftTuple);
		outputTuple.addAll(rightTuple);
		return outputTuple;		
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		left.reset();
		right.reset();
		leftTuple = null;
		rightTuple = null;
	}
	
	public String toString(){
		StringBuilder b = new StringBuilder("CROSS PRODUCT \n");
		Operator childOfRightBranch = this.right;
		
		while(childOfRightBranch != null)
		{
			b.append('\t' +childOfRightBranch.toString() + '\n');
			childOfRightBranch = childOfRightBranch.getChildOp();
		}
		
		return b.toString();
	}
	
	public Operator getChildOp(){
		return this.left;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return this.outputSchema;
	}

	public void setChildOp(Operator child) {		
		this.left = child;		
		left.setParent(this);
		if(this.right != null) this.reset();		
	}
	
	public void setRightOp(Operator child){
		this.right = child;
		right.setParent(this);
		if(this.left != null) this.reset();
	}
	
	public Operator getLeftOperator()
	{
		return left;
	}
	
	public Operator getRightOperator()
	{
		return right;
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
        return this.parentOperator.getUsedColumns();
    }

    private void generateOutputSchema(){
		outputSchema = new HashMap<String, ColumnDetail>();
		leftSchema = new HashMap<String, ColumnDetail>(left.getOutputTupleSchema());
		rightSchema = new HashMap<String, ColumnDetail>(right.getOutputTupleSchema());
		int offset = 0;
		for (Entry<String, ColumnDetail> en : leftSchema.entrySet()){
			String key = en.getKey();
			ColumnDetail value = en.getValue().clone();
			int index = value.getIndex();
			if (index > offset){
				offset = index;
			}
			outputSchema.put(key, value);
		}
		for (Entry<String, ColumnDetail> en : rightSchema.entrySet()){
			String key = en.getKey();
			ColumnDetail value = en.getValue().clone();
			int index = value.getIndex();
			value.setIndex(index + offset + 1);
			outputSchema.put(key, value);
		}
		outputSize = left.getOutputTupleSchema().size() + right.getOutputTupleSchema().size();
	}
	
	
}
