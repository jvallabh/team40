/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

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
	Column[] schema;
	Expression condition;
	
	public SelectionOperator(Operator input, Column[] schema, Expression condition){
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
			String s;
			Evaluator eval = new Evaluator(schema, tuple);
			condition.accept(eval);
			if (!(eval.getBool())) {
				tuple = null;
			}
		} while (tuple == null);
		return tuple;
	}

	@Override
	public void reset() {
		input.reset();	
	}
}
