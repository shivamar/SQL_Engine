package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.DTO.Operator;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by keno on 4/30/15.
 */
public class IndexNLJOperatorL extends JoinOperator {
    private IndexScanOperator indexTable;

    public IndexNLJOperatorL(IndexScanOperator left, Operator right, Expression expr) {
        super(left, right, expr);
        this.indexTable = left;
    }

    @Override
    public ArrayList<Datum> readOneTuple() {

        if(rightTuple == null){
            rightTuple = right.readOneTuple();
            indexTable.setSecondaryKey(rightTuple.get(rightIndex), joinColType);
        }

        ArrayList<Datum> leftTuple = indexTable.readOneTuple();

        if (leftTuple == null){
            // No more duplicates for the said key
            while (rightTuple != null) {
                rightTuple = right.readOneTuple();
                if (rightTuple != null) {
                    indexTable.setSecondaryKey(rightTuple.get(rightIndex), joinColType);
                    leftTuple = indexTable.readOneTuple();
                    if (leftTuple != null){
                        break;
                    }
                }
            }
        }

        if (leftTuple == null || rightTuple == null){
            return null;
        }

        ArrayList<Datum> outputTuple = new ArrayList<Datum>();
        outputTuple.addAll(leftTuple);
        outputTuple.addAll(rightTuple);
        return outputTuple;
    }

    @Override
    public HashSet<String> getUsedColumns() {
        return null;
    }

    @Override
    public String toString() {

        StringBuilder b = new StringBuilder("INDEX NLJ LeftIndexed ON " + this.expr +" WITH \n");

        Operator childOfRightBranch = this.right;

        while(childOfRightBranch != null)
        {
            b.append('\t' +childOfRightBranch.toString() + '\n');
            childOfRightBranch = childOfRightBranch.getChildOp();
        }

        return b.toString();
    }
}
