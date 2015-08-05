/**
 * 
 */
package edu.buffalo.cse562.Operators;

import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.BL.Evaluator;
import edu.buffalo.cse562.BL.Evaluator;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.DTO.Datum;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import edu.buffalo.cse562.Util.Util;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;


public class ExtendedProjection implements Operator {
    Operator input;
    List<SelectItem> SelectItem_List;
    private HashMap<String, ColumnDetail> inputSchema = null;

    // this will be given to SubQueries!
    // the key is (currentColumnWholeName or Expressions or aliases)+"."+index to maintain uniqueness of keys since this operator may contain same column twice in its schema
    // So splice the last two characters separated by "." in SubQuery to make new wholecolumnName. " R.A.0" -> TableAlias of subquery + "A.0"
    private HashMap<String, ColumnDetail> outputSchema = new HashMap<String, ColumnDetail>();
    ArrayList<Datum> inputTuples = null;
    private ArrayList<Datum> outputTuples = new ArrayList<Datum>();
    private Operator parentOperator = null;
    private HashSet<String> referencedColumns;

    public ExtendedProjection(Operator input, List<SelectItem> SelectItem_List) {
        this.SelectItem_List = SelectItem_List;
        referencedColumns = new HashSet<>();
        setChildOp(input);
        setProjectionExpressions(this.SelectItem_List);
    }

    /* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
    @Override
    public ArrayList<Datum> readOneTuple() {
        do {
            inputTuples = input.readOneTuple();
            if (inputTuples == null) return null;

            outputTuples.clear();

            for (SelectItem selectItem : SelectItem_List) {
                if (selectItem instanceof AllColumns) {
                    outputTuples.addAll(inputTuples);
                } else if (selectItem instanceof AllTableColumns) {     // R.*
                    //for a table name R if there exists a column key R.<> pull all the index values in hash set, preventing multiple entries of same columns.
                    // we iterate through the hash set of indexes to all add columns of R to outputTuples
                    Set<Integer> tableColumnIndex = new HashSet<Integer>();

                    String tableName = ((AllTableColumns) selectItem).getTable().getName();
                    for (Entry<String, ColumnDetail> es : inputSchema.entrySet()) {
                        if (es.getKey().contains(tableName)) {
                            tableColumnIndex.add(es.getValue().getIndex());
                        }
                    }
                    for (Iterator<Integer> itr = tableColumnIndex.iterator(); itr.hasNext(); ) {
                        int index = itr.next();
                        outputTuples.add(inputTuples.get(index));
                    }
                } else if (selectItem instanceof SelectExpressionItem) {
                    Expression expr = ((SelectExpressionItem) selectItem).getExpression();
                    if (expr instanceof Function) {
                        String key = expr.toString();
                        // System.out.println(key);
                        if (inputSchema.containsKey(key)) {
                            int index = inputSchema.get(expr.toString()).getIndex(); //.get(column.getWholeColumnName()).getIndex();
                            outputTuples.add(inputTuples.get(index));
                        } else {
                            // Util.printSchema(inputSchema);

                        }
                    } else {
                        Evaluator evaluator = new Evaluator(inputTuples, inputSchema);
                        try {
                            outputTuples.add(new Datum(evaluator.eval(expr)));
                        } catch (SQLException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }

            }

            //     Util.printTuple(outputTuples);
            return outputTuples;
        } while (inputTuples != null);
    }

    private void setOutputSchema() {
        int index = 0;
        for (SelectItem selectItem : SelectItem_List) {
            if (selectItem instanceof AllColumns) {
                // *
                for (Entry<String, ColumnDetail> es : inputSchema.entrySet()) {
                    String oldkey = es.getKey();

                    if (!outputSchema.containsKey(oldkey)) {
                        ColumnDetail colDetail = es.getValue().clone();
                        colDetail.setIndex(index);
                        outputSchema.put(oldkey, colDetail);

                        index++;
                    }
                }
            } else if (selectItem instanceof AllTableColumns) {
                //<tableName>.*
                String tableName = ((AllTableColumns) selectItem).getTable().getName();

                for (java.util.Map.Entry<String, ColumnDetail> es : inputSchema.entrySet()) {
                    String oldKey = es.getKey();
                    if (oldKey.contains(tableName.concat("."))) {
                        if (!outputSchema.containsKey(oldKey)) {
                            ColumnDetail colDetail = inputSchema.get(oldKey).clone();
                            colDetail.setIndex(index);
                            outputSchema.put(oldKey, es.getValue());

                            index++;
                        }
                    }
                }
            } else if (selectItem instanceof SelectExpressionItem) {
                //<columnName> or <columnName> AS <columnAlias> or <columnA + columnB> [AS <columnAlias>]'expression
                String aliasName = ((SelectExpressionItem) selectItem).getAlias();

                //alias name is present!
                if (aliasName != null && !aliasName.isEmpty()) {
                    Expression exp = ((SelectExpressionItem) selectItem).getExpression();
                    String colName = exp.toString();
                    String newKey = aliasName;

                    //R AS RAM,R+A AS RAM the new schema we 'll ve (RAM : ColDetail.IndexNum) if alias is present else {RAM : IndexNum}
                    if (!outputSchema.containsKey(newKey)) {
                        if (inputSchema.containsKey(colName)) {
                            ColumnDetail colDetail = inputSchema.get(colName).clone();
                            colDetail.setIndex(index);

                            //add additional schema for alias names as well
                            outputSchema.put(newKey, colDetail);
                        } else {    //if its a expression... ex: A+B , C*D
                            // a new column not found in previous schema example an arith expression
                            ColumnDetail colDetail = new ColumnDetail();
                            colDetail.setIndex(index);

                            outputSchema.put(newKey, colDetail);
                        }

                        index++;
                    }
                    referencedColumns.addAll(Util.getReferencedColumns(exp));
                } else {
                    //alias name is not present!
                    Expression exp = ((SelectExpressionItem) selectItem).getExpression();
                    String colName = exp.toString();

                    if (!outputSchema.containsKey(colName)) {
                        // an existing column
                        if (inputSchema.containsKey(colName)) {
                            ColumnDetail colDetail = inputSchema.get(colName).clone();
                            colDetail.setIndex(index);
                            outputSchema.put(colName, colDetail);
                        } else { // a new column not found in previous schema example an arith expression
                            // //if its a expression... A+B , C*D
                            ColumnDetail colDetail = new ColumnDetail();
                            colDetail.setIndex(index);

                            outputSchema.put(colName, colDetail);
                        }

                        index++;
                    }
                    referencedColumns.addAll(Util.getReferencedColumns(exp));
                }

            }
        }
    }

    /* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
    //sets the outputTupleSchema
    @Override
    public void reset() {
        this.inputSchema = input.getOutputTupleSchema();
        setOutputSchema();
    }

    public String toString() {
        String str = "";
        try {
            str = "Extended Projection Operator: " + this.SelectItem_List.toString();
        } catch (Exception ex) {
            System.err.println("Error in printing data extended projection:");
            if (this.SelectItem_List == null) {
                System.err.println("this.SelectItem_List is null");
            }
            throw ex;
        }
        return str;
    }

    public Operator getChildOp() {
        return this.input;
    }

    public void setProjectionExpressions(List<SelectItem> SelectItem_List) {
        if (SelectItem_List == null) {
            System.err.println("Selection list in selection operator is null");
        }
        this.SelectItem_List = SelectItem_List;

        setOutputSchema();
    }


    @Override
    public HashMap<String, ColumnDetail> getOutputTupleSchema() {
        return outputSchema;
    }

    public void setChildOp(Operator child) {
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
        referencedColumns.addAll(outputSchema.keySet());
        return referencedColumns;
    }
}
