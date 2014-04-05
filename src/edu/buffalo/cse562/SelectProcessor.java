/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class SelectProcessor {
	/**
	 * It processes the select statement and returns the final operator that you can use to print to console.
	 * @param selectStmt
	 * @return
	 */
	static Operator processPlainSelect(PlainSelect pselect){
		Expression selectCondition = pselect.getWhere();
		ArrayList[] partitionedConditions = Util.partitionWhereClause(selectCondition);
		ArrayList<Expression> whereCondExpressions = partitionedConditions[0];
		ArrayList<Expression> conditionsOnSingleTables = partitionedConditions[1];
		//System.out.println("Extracted conditions affecting single table: "+Util.conditionsOnSingleTables);
		
		List<SelectItem> selectItems= pselect.getSelectItems();
		FromScanner fromscan = new FromScanner(Main.dataDir, Main.tables);
		pselect.getFromItem().accept(fromscan);
		Operator firstTableOperator = fromscan.source;
		
		//Incase of subSelect, we will get final resultant operator like project operator.
		if(firstTableOperator instanceof ScanOperator){
			((ScanOperator)firstTableOperator).conditions = Util.getConditionsOfTable(firstTableOperator.getSchema(), conditionsOnSingleTables);
		}		
									
		/*
		 * If JOIN is present in the SQL query then first we are fetching the list of JOIN tables.
		 * Then for each table we are joining them based on the join condition.
		 */
		List<Join> joinDetails = pselect.getJoins();
		boolean hasJoin = joinDetails == null?false:true;
		JoinOperator finalJoinedOperator = null;
		
		List<Column> groupByColumns = pselect.getGroupByColumnReferences();
		
		for(SelectItem selectItem:selectItems){
			Expression expr = ((SelectExpressionItem)selectItem).getExpression();
			if(expr instanceof Function) {
				Function count = (Function) expr;
				if(count.getName().equalsIgnoreCase("COUNT")){
					if(count.isDistinct()) {
						ExpressionList exp = count.getParameters();
						List<Expression> expList = exp.getExpressions();
						String colname = ((SelectExpressionItem)selectItem).getAlias();
						Column col = (Column)expList.get(0);
						if(colname!=null)
							col = new Column(new Table(),colname);
						groupByColumns.add(col);
					}
				}
			}
		}
		
		boolean hasGroupBy = groupByColumns == null?false:true;
		Operator finalGrpByOperator = null;
		
		List<OrderByElement> orderByColumns = pselect.getOrderByElements();
		boolean hasOrderBy = orderByColumns == null?false:true;
		Operator finalOrderByOperator = null;
		
		Distinct distinct = pselect.getDistinct();
		boolean hasDistinct = distinct == null?false:true;
		
		DistinctOperator distinctOperator = null;
		
		SelectionOperator selectOperator = null;
		ProjectionOperator projectOperator = null;
		
		if(hasJoin){			
			if(Main.swapDir == null){
				finalJoinedOperator = (JoinOperator) Util.getJoinedOperatorHashHybrid(firstTableOperator, joinDetails, conditionsOnSingleTables, whereCondExpressions);				
			}
			else{
				finalJoinedOperator = (JoinOperator) Util.getJoinedOperatorExternal(firstTableOperator, joinDetails, conditionsOnSingleTables, whereCondExpressions);
			}
			selectOperator = new SelectionOperator(finalJoinedOperator, finalJoinedOperator.schema, whereCondExpressions);
		
		}
		else{			
			selectOperator = new SelectionOperator(firstTableOperator, firstTableOperator.getSchema(), whereCondExpressions);
		}
		
		projectOperator = new ProjectionOperator(selectOperator,selectOperator.getSchema(),selectItems);
		
		if(hasGroupBy){
			finalGrpByOperator = Util.getGroupByOperator(projectOperator, groupByColumns);
		}
		Operator inputToAggr = finalGrpByOperator!=null?finalGrpByOperator:projectOperator;

		if(hasDistinct){
			List<SelectItem> distinctColumns = distinct.getOnSelectItems();
			distinctOperator = new DistinctOperator(inputToAggr, distinctColumns);
		}
		
		inputToAggr = distinctOperator != null?distinctOperator:inputToAggr;
		
		AggrOperator aggrOperator = new AggrOperator(inputToAggr,inputToAggr.getSchema(),selectItems);
		
		if(hasOrderBy && !Util.isOrderBySameAsGroupBy(orderByColumns, groupByColumns)){
			if(Main.swapDir==null)
				finalOrderByOperator = new OrderByOperator(aggrOperator, orderByColumns);
			else
				finalOrderByOperator = new ExternalSortOperator(aggrOperator, orderByColumns, Main.swapDir, -1);
		}
		Operator finalOperator = finalOrderByOperator != null?finalOrderByOperator:aggrOperator;
		
		
		/*if(hasGroupBy && hasOrderBy){
			finalOperator = Util.getGroupByOperator(finalOperator, groupByColumns);
		}*/
		return (Operator) finalOperator;		
  }	
	
}
