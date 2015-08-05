/**
 * 
 */
package edu.buffalo.cse562.DTO;

import edu.buffalo.cse562.DTO.ColumnDetail;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author Sathish
 *
 */
public interface Operator {
	
	/**
	 * returns one tuple at a time
	 * @return
	 */
	public ArrayList<Datum> readOneTuple();
	
	/**
	 * Returns the output tuple schema the implementer may produce
	 * @return
	 */
	public HashMap<String,ColumnDetail> getOutputTupleSchema();
	
	/**
	 * resets the iterator to the initial item
	 */
	public void reset();
	
	/**
	 * Returns its child operator
	 * @return
	 */
	public Operator getChildOp();
		
	/***
	 * Sets the child Operator 
	 */
	public void setChildOp(Operator child);
	
	/**
	 * Gets the parent Operator
	 * @param parent
	 */
	public Operator getParent();
	
	/**
	 * Sets the parent Operator
	 * @param parent
	 */
	public void setParent(Operator parent);

    public HashSet<String> getUsedColumns();
}
