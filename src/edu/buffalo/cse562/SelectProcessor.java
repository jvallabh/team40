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
		
		SelectionOperator selectOperator = null;
		ProjectionOperator projectOperator = null;
		
		if(hasJoin){
			finalJoinedOperator = (JoinOperator) Util.getJoinedOperator(firstTableOperator, joinDetails);
			selectOperator = new SelectionOperator(finalJoinedOperator, finalJoinedOperator.schema, selectCondition);
		}
		else{			
			selectOperator = new SelectionOperator(firstTableOperator, firstTableOperator.getSchema(), selectCondition);
		}
		
		if(hasGroupBy){
			finalGrpByOperator = (GroupByOperator) Util.getGroupByOperator(selectOperator, groupByColumns);
		}
		
		Operator inputToProject = finalGrpByOperator!=null?finalGrpByOperator:selectOperator;
		projectOperator = new ProjectionOperator(inputToProject,inputToProject.getSchema(),selectItems);
		return (Operator) projectOperator;		
  }	
	
}