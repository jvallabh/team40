/**
 * 
 */
package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class IndexScanOperator implements Operator {
	BufferedReader input;
	File f;
	ColumnInfo[] schema;
	ArrayList<Expression> conditions = new ArrayList<Expression>();
	Evaluator eval;
	Iterator<Expression> iterator;
	Expression currCondition;
	Expression indexCondition;
	boolean hasIndex;
	
	public IndexScanOperator(File f, ColumnInfo[] schema) {
		this.f = f;
		this.schema = schema;
		reset(); 
		this.eval = new Evaluator(schema);
	}
	
	@Override
	public Datum[] readOneTuple() {
		if (hasIndex) {
			//have to send the tuples based on Index
			return null;
		} else {
			if (input == null) return null;
			String line = null;
			Datum[] ret = null;
			while(true){
				try {
					line = input.readLine();
				} catch(IOException e) {
					e.printStackTrace();
				}
				if (line == null) return null;
				String[] cols = line.split("\\|");
				ret = new Datum[cols.length];
				for (int i=0; i<cols.length; i++) {
					//ret[i] = new Datum.Long(cols[i]);
					ret[i] = new Datum(cols[i]);
				}
				if(evaluateTuple(ret)){
					break;
				}			
			}
			return ret;
		}
	}

	@Override
	public void reset() {
		try {
			if(input != null){
				input.close();
			}
			input = new BufferedReader(new FileReader(f));
			indexCondition = getIndexCondition();
		} catch (IOException e) {
			e.printStackTrace();
			input = null;
		}
	}

	public Expression getIndexCondition() {
		hasIndex = false;
		if(conditions.size() == 0) {
			return null;
		} else {
			iterator = conditions.iterator();
			while (iterator.hasNext()) {
				Expression currExp = iterator.next();
				if (currExp instanceof EqualsTo) {
					hasIndex = true;
					return currExp;
				}
			}
			iterator = conditions.iterator();
			while (iterator.hasNext()) {
				Expression currExp = iterator.next();
				if ((currExp instanceof MinorThan)
					|| (currExp instanceof MinorThanEquals)
					|| (currExp instanceof GreaterThan)
					|| (currExp instanceof GreaterThanEquals)) {
					hasIndex = true;
					return currExp;
				}
			}
			return null;
		}
	}
	
	@Override
	public ColumnInfo[] getSchema() {
		return schema;
	}

	private boolean evaluateTuple(Datum[] tuple){
		boolean result = true;
		if(conditions.size() != 0){
			iterator = conditions.iterator();
			while(iterator.hasNext()){
				currCondition = iterator.next();				
				eval.sendTuple(tuple);
				currCondition.accept(eval);
					if (!(eval.getBool())) {
						result = false;
						break;
					}				
			}
		}
		return result;
	}
}