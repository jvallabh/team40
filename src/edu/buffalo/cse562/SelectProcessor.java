/**
 * 
 */
package edu.buffalo.cse562;

import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
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
		Util.partitionWhereClause(selectCondition);
		//System.out.println("Extracted conditions affecting single table: "+Util.conditionsOnSingleTables);
		
		List<SelectItem> selectItems= pselect.getSelectItems();
		FromScanner fromscan = new FromScanner(Main.dataDir, Main.tables);
		pselect.getFromItem().accept(fromscan);
		Operator firstTableOperator = fromscan.source;
									
		/*
		 * If JOIN is present in the SQL query then first we are fetching the list of JOIN tables.
		 * Then for each table we are joining them based on the join condition.
		 */
		List<Join> joinDetails = pselect.getJoins();
		boolean hasJoin = joinDetails == null?false:true;
		JoinOperator finalJoinedOperator = null;
		
		List<Column> groupByColumns = pselect.getGroupByColumnReferences();
		boolean hasGroupBy = groupByColumns == null?false:true;
		GroupByOperator finalGrpByOperator = null;
		
		List<OrderByElement> orderByColumns = pselect.getOrderByElements();
		boolean hasOrderBy = orderByColumns == null?false:true;
		OrderByOperator finalOrderByOperator = null;
		
		SelectionOperator selectOperator = null;
		ProjectionOperator projectOperator = null;
		
		if(hasJoin){
			finalJoinedOperator = (JoinOperator) Util.getJoinedOperator(firstTableOperator, joinDetails);
			selectOperator = new SelectionOperator(finalJoinedOperator, finalJoinedOperator.schema, selectCondition);
		}
		else{			
			selectOperator = new SelectionOperator(firstTableOperator, firstTableOperator.getSchema(), selectCondition);
		}
		
		projectOperator = new ProjectionOperator(selectOperator,selectOperator.getSchema(),selectItems);
		
		if(hasGroupBy){
			finalGrpByOperator = (GroupByOperator) Util.getGroupByOperator(projectOperator, groupByColumns);
		}
		Operator intputToAggr = finalGrpByOperator!=null?finalGrpByOperator:projectOperator;

		AggrOperator aggrOperator = new AggrOperator(intputToAggr,intputToAggr.getSchema(),selectItems);
		
		if(hasOrderBy){
			finalOrderByOperator = new OrderByOperator(aggrOperator, orderByColumns);
		}
		Operator finalOperator = finalOrderByOperator != null?finalOrderByOperator:aggrOperator;
		
		if(hasGroupBy && hasOrderBy){
			finalOperator = (GroupByOperator) Util.getGroupByOperator(finalOperator, groupByColumns);
		}
		return (Operator) finalOperator;		
  }	
	
}
