/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.Evaluator.Type;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class ProjectionOperator implements Operator {
	Operator input;
	ColumnInfo[] schema;
	List<SelectItem> selectItems;
	ArrayList<Integer> itemList = new ArrayList<>();
	ColumnInfo[] finalSchema;
	private static int sum=1,avg=2,count=3,min=4,max=5;
	
	public ProjectionOperator(Operator input, ColumnInfo[] schema, List<SelectItem> selectItems) {
		this.input = input;
		this.schema = schema;
		this.selectItems = selectItems;
		this.finalSchema = changeSchema(selectItems);
	}
	
	public int getColumnID(Column col) {
		for (int i=0; i<schema.length; i++) {
			if (schema[i].colDef.getColumnName().equals(col.getColumnName())) {
				return i;
			}
		}
		return -1;
	}
	
	public ColDataType getNumType(Expression expr) {
		if (expr instanceof Parenthesis) {
			return getNumType(((Parenthesis) expr).getExpression());
		}
		
		if (expr instanceof BinaryExpression) {
			ColDataType colDataType = getNumType(((BinaryExpression) expr).getLeftExpression());
			if (colDataType == null) {
				return getNumType(((BinaryExpression) expr).getRightExpression());
			}
			return colDataType;	
		}
		else if (expr instanceof Column) {
			Column col = (Column) expr;
			return (schema[getColumnID(col)].colDef.getColDataType());
		}
		else if (expr instanceof Function) {
			if(((Function) expr).getName().equalsIgnoreCase("count")) {
				ColDataType colDataType =  new ColDataType();
				colDataType.setDataType("int");
				return colDataType;
			}
			else if(((Function) expr).getName().equalsIgnoreCase("avg")) {
				ColDataType colDataType =  new ColDataType();
				colDataType.setDataType("double");
				return colDataType;
			}
			else {
				List expList = ((Function) expr).getParameters().getExpressions();
				for(int i=0;i<expList.size();i++){
					if(expList.get(i) instanceof Column){
						Column col = (Column) expList.get(i);
						return (schema[getColumnID(col)].colDef.getColDataType());
					}
				}
				return getNumType((Expression)expList.get(0));
			}
		}
		return null;
	}
	
	public String getTableName(Expression expr) {
		if (expr instanceof Column) {
			return ((Column) expr).getTable().getName();
		}
		else 
			return new String("dummy");
	}
	
	public int isFunction(Function f) {
		int function=0;
		if(f.getName().toString().equalsIgnoreCase("SUM"))
	            function =sum;
	    if(f.getName().toString().equalsIgnoreCase("MIN"))
	    		function =min;
	    if(f.getName().toString().equalsIgnoreCase("MAX"))
	    		function =max;
	    if(f.getName().toString().equalsIgnoreCase("AVG"))
	    		function =avg;
	    if(f.getName().toString().equalsIgnoreCase("COUNT"))
	    		function =count;
	    else 
	            function =0;
	    return function;
	}
	public ColumnInfo[] changeSchema(List selectItems) {
		ColumnInfo[] schema;
		ArrayList<ColumnInfo> schemaList = new ArrayList();
		SelectExpressionItem exp;
		for (int j=0; j<selectItems.size(); j++) {
			if (selectItems.get(j) instanceof AllColumns || selectItems.get(j) instanceof AllTableColumns)
				schemaList = appendAllColumns(schemaList, (SelectItem) selectItems.get(j));
			else {
				ColumnDefinition colDef = new ColumnDefinition();
				exp = (SelectExpressionItem)selectItems.get(j);
				if(exp.getAlias()!=null) {
					colDef.setColumnName(exp.getAlias());
				}
				else {
					colDef.setColumnName(exp.toString());
				}
				colDef.setColDataType(getNumType(exp.getExpression()));
				if(exp.getExpression() instanceof Function) {
					schemaList.add(new ColumnInfo(colDef, getTableName(exp.getExpression()),isFunction((Function) exp.getExpression())));
				}
				else
					schemaList.add(new ColumnInfo(colDef, getTableName(exp.getExpression()),0));
			}
		}
		schema = schemaList.toArray(new ColumnInfo[0]);
		return schema;
	}
	
	public ArrayList appendAllColumns(ArrayList schemaList, SelectItem AllColumns) {
		if(AllColumns instanceof AllColumns) {
			for(int i=0;i<schema.length;i++) {
					schemaList.add(schema[i]);
			}
		}
		else {
			Table t = ((AllTableColumns) AllColumns).getTable();
			for(int i=0;i<schema.length;i++) {
				if(t.getName().equals(schema[i].tableName))
					schemaList.add(schema[i]);
			}
		}
		return schemaList;
	}
	
	@Override
	public Datum[] readOneTuple() {
		Datum[] tuple = input.readOneTuple();
		ArrayList<Datum> tupleList = new ArrayList<>();
		Datum[] newTuple = null;
		if(tuple == null)
			return null;
		if (selectItems.get(0) instanceof AllColumns)
			return tuple;
		else {			
			SelectExpressionItem selectExp;
			Expression exp = null;
			for (int j=0; j<selectItems.size(); j++) {
				selectExp = (SelectExpressionItem)selectItems.get(j);
				exp = selectExp.getExpression();
				Evaluator eval = new Evaluator(schema, tuple);
				exp.accept(eval);
				tupleList.add(new Datum(eval.getValue()));
			}
			newTuple = tupleList.toArray(new Datum[0]);
			return newTuple;
		}
	}

	@Override
	public void reset() {
		input.reset();
	}

	@Override
	public ColumnInfo[] getSchema() {
		// TODO Auto-generated method stub
		return finalSchema;
	}
}
