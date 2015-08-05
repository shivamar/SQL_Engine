package edu.buffalo.cse562.Util;

import edu.buffalo.cse562.Operators.IndexScanOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;

/**
 * Created by keno on 4/30/15.
 */
public class IndexScanConfig {
    public IndexScanOperator.SearchMode getSearchMode() {
        return searchMode;
    }

    public void setSearchMode(IndexScanOperator.SearchMode searchMode) {
        this.searchMode = searchMode;
    }

    public LeafValue getValue() {
        return value;
    }

    public void setValue(LeafValue value) {
        this.value = value;
    }

    private IndexScanOperator.SearchMode searchMode;
    private LeafValue value;

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

    private Expression expr;
}
