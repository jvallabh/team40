/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;

import net.sf.jsqlparser.expression.BinaryExpression;
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
	ColumnInfo[] schema;
	ArrayList<Expression> condition;
	Evaluator eval;
	Expression currExp;
	
	public SelectionOperator(Operator input, ColumnInfo[] schema, ArrayList<Expression> condition){
		this.input = input;
		this.condition = condition;
		this.schema = schema;
		this.eval = new Evaluator(schema);
	}

	@Override
	public Datum[] readOneTuple() {
		Datum[] tuple = null;
		do {
			tuple = input.readOneTuple();
			if (tuple == null) return null;
			
			eval.sendTuple(tuple);
			if(condition.size() != 0){
				Iterator<Expression> iterator = condition.iterator();
				while(iterator.hasNext()){
					currExp = iterator.next();
					currExp.accept(eval);
					if (!(eval.getBool())) {
						tuple = null;
						break;
					}
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
