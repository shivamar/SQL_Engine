/**
 * 
 */
package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Datum;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import net.sf.jsqlparser.statement.select.Limit;

/**
 * @author Sathish
 *
 */
public class LimitOperator implements Operator {

	private Limit limit;
	private long counter;
	private Operator source;
	
	private Operator parentOperator = null;

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
	public ArrayList<Datum> readOneTuple() {
		// TODO Auto-generated method stub
		if (limit.getOffset() > counter) {
			counter++;
			return readOneTuple();
		}		
		if (limit.isLimitAll()){
			return this.source.readOneTuple();
		}
		else if (counter < limit.getRowCount()){
			counter++;
			return this.source.readOneTuple();
		}
		return null;
	}
	

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {

	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "LIMIT " + this.limit.getRowCount() + "\n" + this.source.toString();
	}


	@Override
	public Operator getChildOp() {
		return this.source;
	}


	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		return null;
	}
	
	public LimitOperator(Operator input, Limit limitObj){
		this.limit = limitObj;
		this.counter = 0;
		this.source = input;
	}
	
	public void setChildOp(Operator child) {		
		this.source = child;
		this.source.setParent(this);	
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

}
