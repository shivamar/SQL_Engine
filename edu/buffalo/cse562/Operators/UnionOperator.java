package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Datum;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class UnionOperator implements Operator {
	List<Operator> operators;
	HashSet<String> tuplesSeen;
	
	@Override
	public ArrayList<Datum> readOneTuple() {
		// TODO Auto-generated method stub
		for (Operator op : operators){
			ArrayList<Datum> res = op.readOneTuple();
			if (res != null){
				if(tuplesSeen.contains(res.toString())){
					return readOneTuple();
				}
				else{
					tuplesSeen.add(res.toString());
					return res;
				}
			}
		}
		return null;
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.tuplesSeen = new HashSet<String>();
	}
	
	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return this.operators.get(0).getOutputTupleSchema();
	}
	
	public UnionOperator(){
		this.operators = new ArrayList<Operator>();
		this.tuplesSeen = new HashSet<String>();
	}
	
	public void addOperator(Operator op) {
		this.operators.add(op);		
	}

	@Override
	public Operator getParent() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setParent(Operator parent) {
		//null		
	}

    @Override
    public HashSet<String> getUsedColumns() {
        return null;
    }

    @Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setChildOp(Operator child)
	{
		//null
	}
}
