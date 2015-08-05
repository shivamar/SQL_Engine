package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.Util.Util;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by keno on 4/30/15.
 */
public class IndexNLJOperatorR extends JoinOperator {
    private IndexScanOperator indexTable;
    int tupleCount;

    public IndexNLJOperatorR(Operator left, IndexScanOperator right,  Expression expr) {
        super(left, right, expr);
        this.indexTable = right;
    }

    @Override
    public ArrayList<Datum> readOneTuple() {

        if(leftTuple == null){
            leftTuple = left.readOneTuple();
            indexTable.setSecondaryKey(leftTuple.get(leftIndex), joinColType);
        }

        ArrayList<Datum> rightTuple = indexTable.readOneTuple();

        if (rightTuple == null){
            // No more duplicates for the said key
            while (leftTuple != null) {
                leftTuple = left.readOneTuple();
                if (leftTuple != null) {
                    indexTable.setSecondaryKey(leftTuple.get(leftIndex), joinColType);
                    rightTuple = indexTable.readOneTuple();
                    if (rightTuple != null) {
                        break;
                    }
                }
            }
        }

        if (rightTuple == null || leftTuple == null){
            return null;
        }

        ArrayList<Datum> outputTuple = new ArrayList<Datum>();
        outputTuple.addAll(leftTuple);
        outputTuple.addAll(rightTuple);
        tupleCount++;
        return outputTuple;
    }

    @Override
    public HashSet<String> getUsedColumns() {
        HashSet<String> usedColumns = this.parentOperator.getUsedColumns();
        usedColumns.addAll(Util.getReferencedColumns(expr));
        return usedColumns;
    }

    @Override
    public String toString() {

        StringBuilder b = new StringBuilder("INDEX NLJ RightIndexed ON " + this.expr +" WITH \n");

        Operator childOfRightBranch = this.right;

        while(childOfRightBranch != null)
        {
            b.append('\t' +childOfRightBranch.toString() + '\n');
            childOfRightBranch = childOfRightBranch.getChildOp();
        }

        return b.toString();
    }


}
