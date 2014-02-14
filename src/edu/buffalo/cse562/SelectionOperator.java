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
	Expression condition;
	
	public SelectionOperator(Operator input, Expression condition){
		this.input = input;
		this.condition = condition;
	}

	@Override
	public void resetStream() {
		input.resetStream();
		
	}

	@Override
	public Tuple readOneTuple() {
		Tuple t = null;
		do{
			t = input.readOneTuple();
			/*if(!evaluate(t, condition)){
				t = null;
			}*/
		}
		while(t==null);
		return t;
	}
}
