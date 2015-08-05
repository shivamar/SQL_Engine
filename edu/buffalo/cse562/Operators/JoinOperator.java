package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.BL.Evaluator;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.Util.Util;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;

public class JoinOperator implements Operator {
	protected Operator left;
	protected Operator right;
	protected HashMap<String, ColumnDetail> outputSchema = null;
	protected HashMap<String, ColumnDetail> leftSchema;
	protected HashMap<String, ColumnDetail> rightSchema;
	protected ArrayList<Datum> leftTuple;
	protected ArrayList<Datum> rightTuple;

    public Expression getExpr() {
        return expr;
    }

    protected Expression expr;

    public int getLeftIndex() {
        return leftIndex;
    }

    public int getRightIndex() {
        return rightIndex;
    }

    protected int leftIndex;
	protected int rightIndex;
	protected Operator parentOperator;
	protected int divider;
    protected String joinColType;
	
	
	public JoinOperator(Operator left, Operator right, Expression expr){
		this.left = left;
		this.right = right;
		this.expr = expr;
        generateOutputSchema();
        setChildOp(left);
        setRightOp(right);
		initLRIndexes();
    }

    public void initLRIndexes() {
        String[] fields = expr.toString().split("=");
        //Test left, then right
        ColumnDetail cd = left.getOutputTupleSchema().get(fields[0].trim());
        if (cd == null) {
            cd = Evaluator.getColumnDetail(left.getOutputTupleSchema(), fields[1].trim().toLowerCase());
//                System.out.println(cd);
//                System.out.println(fields[0]);
//                System.out.println(left.getOutputTupleSchema());
//                System.out.println(right.getOutputTupleSchema());
            leftIndex = cd.getIndex();
            ColumnDetail col = Evaluator.getColumnDetail(right.getOutputTupleSchema(), fields[0].trim().toLowerCase());
//            System.out.println(col);
            if (col != null) {
                rightIndex = col.getIndex();
            }
        } else {
            leftIndex = cd.getIndex();
            try {
                rightIndex = Evaluator.getColumnDetail(right.getOutputTupleSchema(), fields[1].trim()).getIndex();
            } catch (Exception ex) {
                System.err.println("Error in join while trying to access the index of :" + fields[1].trim());
//                Util.printSchema(right.getOutputTupleSchema());
                System.err.println("column not present in schema");
            }
        }
        joinColType = cd.getColumnDefinition().getColDataType().getDataType();
    }
	
	@Override
	public ArrayList<Datum> readOneTuple() {
		return null;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		//System.out.println("-------------------");
		//System.out.println("-------------------");
		return outputSchema;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		left.reset();
		right.reset();
		generateOutputSchema();
	}

	@Override
	public void setChildOp(Operator child) {
        this.left = child;
		left.setParent(this);
//        System.out.println("Setting left");
//        System.out.println(expr);
//        System.out.println(left);
//        System.out.println(right);
//        System.out.println("____");
        if (right != null) {
            generateOutputSchema();
            initLRIndexes();
        }
		//reset();
	}
	
	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return this.left;
	}
	
	public void setRightOp(Operator child){
		this.right = child;
		right.setParent(this);
//        System.out.println("Setting right");
//        System.out.println(expr);
//        System.out.println(left);
//        System.out.println(right);
//        System.out.println("____");
		if(this.left != null){
            generateOutputSchema();
            initLRIndexes();
        }
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
        return null;
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
		this.divider = offset;
	}
	
	
}
