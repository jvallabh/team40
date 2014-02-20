/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class SelectionOperator implements Operator {
	Operator input;
	ColumnInfo[] schema;
	Expression condition;
	
	public SelectionOperator(Operator input, ColumnInfo[] schema, Expression condition){
		this.input = input;
		this.condition = condition;
		this.schema = schema;
	}

	@Override
	public Datum[] readOneTuple() {
		Datum[] tuple = null;
		do {
			tuple = input.readOneTuple();
			if (tuple == null) return null;
			
			Evaluator eval = new Evaluator(schema, tuple);
			if(condition != null){
				condition.accept(eval);
				if (!(eval.getBool())) {
					tuple = null;
				}				
			}
		} while (tuple == null);
		return tuple;
	}

	@Override
	public void reset() {
		input.reset();	
	}

	@Override
	public ColumnInfo[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}
}
