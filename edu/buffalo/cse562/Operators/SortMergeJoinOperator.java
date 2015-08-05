package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.Operators.JoinOperator;
import edu.buffalo.cse562.DTO.Datum;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import net.sf.jsqlparser.expression.Expression;

public class SortMergeJoinOperator extends JoinOperator{
	Comparator<ArrayList<Datum>> comp;
	boolean iterate = true;
	LinkedList<ArrayList<Datum>> leftBuffer;
	LinkedList<ArrayList<Datum>> rightBuffer;
	LinkedList<ArrayList<Datum>> outputBuffer;
	ArrayList<Datum> lastMatched;
	Iterator<ArrayList<Datum>> currentBag;
	boolean started = false;


	public SortMergeJoinOperator(Operator left, Operator right, Expression expr){
		super(left, right, expr);
		outputBuffer = new LinkedList<ArrayList<Datum>>();
		leftBuffer = new LinkedList<ArrayList<Datum>>();
		rightBuffer = new LinkedList<ArrayList<Datum>>();

		currentBag = outputBuffer.iterator();
	}

	@Override
	public ArrayList<Datum> readOneTuple() {
		//try to match more, if the current list is empty		
		if (!currentBag.hasNext()){
			getNewBag();
		}

		if (currentBag.hasNext()){
			return currentBag.next();
		}

		return null;
	}

	public void setComparator(Comparator<ArrayList<Datum>> comp){
		this.comp = comp;
	}

	private void getNewBag(){
		if (!started) {
			leftTuple = left.readOneTuple();
			rightTuple = right.readOneTuple();
			started = true;
		}
		outputBuffer = new LinkedList<ArrayList<Datum>>();

		while (!(leftTuple == null) && !(rightTuple == null)){
			leftBuffer = new LinkedList<ArrayList<Datum>>();
			rightBuffer = new LinkedList<ArrayList<Datum>>();
			int diff = leftTuple.get(leftIndex).compareTo(rightTuple.get(rightIndex));
			//			System.out.println(" Comparing " + leftTuple.get(leftIndex) + " and " +rightTuple.get(rightIndex));
			if (diff == 0){
				//				System.out.println("=== Matched!");
				leftBuffer.add(leftTuple);
				int diff2 = 0;
				while (diff2 == 0 && rightTuple != null){
					rightBuffer.add(rightTuple);
					rightTuple = right.readOneTuple();
					if (rightTuple != null){
						diff2 = leftTuple.get(leftIndex).compareTo(rightTuple.get(rightIndex));
					}
				}

				ArrayList<Datum> leftTemp = left.readOneTuple();
				if (leftTemp != null && leftTuple != null){
					int diff3 = leftTuple.get(leftIndex).compareTo(leftTemp.get(leftIndex));
					while (diff3 == 0 && leftTemp != null){
						leftBuffer.add(rightTuple);
						leftTuple = left.readOneTuple();
						if (leftTuple != null){
							diff2 = leftTuple.get(leftIndex).compareTo(leftTemp.get(leftIndex));
						}
						//							System.out.println("Matched multiple on left!");
					}
				}
				leftTuple = leftTemp;
				miniCross();
				return;
			}
			else if (diff < 0){
				leftTuple = left.readOneTuple();
				//				System.out.println("+++ Moving on");
			}
			else{
				rightTuple = right.readOneTuple();
				//				System.out.println("+++ Moving on");
			}
		}
	}

	public void miniCross() {
		//		System.out.println("Crossing " + leftBuffer.size()+ " and " +rightBuffer.size());
		for (ArrayList<Datum> leftIter : leftBuffer){

			for (ArrayList<Datum> rightIter : rightBuffer){
				ArrayList<Datum> outputTuple = new ArrayList<Datum>(this.getOutputTupleSchema().size());
				outputTuple.addAll(leftIter);
				outputTuple.addAll(rightIter);
				outputBuffer.add(outputTuple);
			}
		}
		leftBuffer = null;
		rightBuffer = null;
		System.gc();

		//		System.out.println("Buffer size now " +outputBuffer.size());
		currentBag = outputBuffer.iterator();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub

		StringBuilder b = new StringBuilder("SORT MERGE JOIN ON " + this.expr +" WITH \n");

		Operator childOfRightBranch = this.right;

		while(childOfRightBranch != null)
		{
			b.append('\t' +childOfRightBranch.toString() + '\n');
			childOfRightBranch = childOfRightBranch.getChildOp();
		}

		return b.toString();
	}
}