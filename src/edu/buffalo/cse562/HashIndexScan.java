package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.ListIterator;

import jdbm.RecordManager;
import jdbm.btree.BTree;
import jdbm.helper.TupleBrowser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;

public class HashIndexScan implements Operator{
	BufferedReader inputScan;
	File f;
	ColumnInfo[] schema;
	ArrayList<Expression> conditions = new ArrayList<Expression>();
	Evaluator eval;
	Iterator<Expression> iterator;
	Expression currCondition;
	Expression indexCondition;
	int indexType=-1;
	int index;
	boolean exitCondition;
	Iterator<String[]> iter;
	String[] iterList;
	LinkedHashMap<String, ArrayList<String[]>> map = new LinkedHashMap<String, ArrayList<String[]>>();
	int where = 0;
	
	public HashIndexScan(File f, ColumnInfo[] schema){
		this.f = f;
		this.schema = schema;
		reset();
		this.eval = new Evaluator(schema);
	}
	
	
	public void processHashIndex(){
		indexCondition = getIndexCondition();
		if(indexType!=-1){
			try {
				File file = new File(Main.indexDir+schema[index].tableName+schema[index].colDef.getColumnName());
				FileInputStream f = new FileInputStream(file);
			    ObjectInputStream s = new ObjectInputStream(f);
			    map = new LinkedHashMap<String, ArrayList<String[]>>();
			    map = (LinkedHashMap<String, ArrayList<String[]>>)s.readObject();
			    s.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}
			iterList = new String[map.size()];
			map.keySet().toArray(iterList);
				if(indexType == 0){
					((EqualsTo)indexCondition).getRightExpression().accept(eval);
					where = new ArrayList(map.keySet()).indexOf(eval.getValue());
				}
				else if(indexType == 1){
					((MinorThan)indexCondition).getRightExpression().accept(eval);
					where = searchKey(eval.getValue());
					if(iterList[where]==eval.getValue())
					where--;
				}
				else if(indexType == 2){
					((MinorThanEquals)indexCondition).getRightExpression().accept(eval);
					where = searchKey(eval.getValue());
				}
				else if(indexType == 3){
					((GreaterThan)indexCondition).getRightExpression().accept(eval);
					where = searchKey(eval.getValue());
					where++;
				}
				else{
					((GreaterThanEquals)indexCondition).getRightExpression().accept(eval);
					where = searchKey(eval.getValue());
					if(iterList[where]!=eval.getValue())
					where++;
				}
		}	
	}
	
	public Expression getIndexCondition() {
		indexType = -1;
		if(conditions.size() == 0) {
			return null;
		} else {
			iterator = conditions.iterator();
			while (iterator.hasNext()) {
				Expression currExp = iterator.next();
				if (currExp instanceof EqualsTo) {
					indexType = 0;
					index = eval.getColumnID((Column) ((EqualsTo)currExp).getLeftExpression());
							
					return currExp;
				}
			}
			iterator = conditions.iterator();
			while (iterator.hasNext()) {
				Expression currExp = iterator.next();
				if (currExp instanceof MinorThan) {
					indexType = 1;
					index = eval.getColumnID((Column) ((MinorThan)currExp).getLeftExpression());
					iterator.remove();
					return currExp;					
				} else if (currExp instanceof MinorThanEquals) {
					indexType = 2;
					index = eval.getColumnID((Column) ((MinorThanEquals)currExp).getLeftExpression());
					iterator.remove();
					return currExp;					
				} else if (currExp instanceof GreaterThan) {
					indexType = 3;
					index = eval.getColumnID((Column) ((GreaterThan)currExp).getLeftExpression());
					iterator.remove();
					return currExp;					
				} else if (currExp instanceof GreaterThanEquals) {
					indexType = 4;
					index = eval.getColumnID((Column) ((GreaterThanEquals)currExp).getLeftExpression());
					iterator.remove();
					return currExp;
				}
			}
			return null;
		}
	}
	
	@Override
	public Datum[] readOneTuple() {
		Datum[] tuple=null;
		if(indexType!=-1){
			while(true) {
			try{
				if (indexType == 1||indexType == 2)
					tuple = lessThanTuple();
				else if (indexType == 3 || indexType == 4)
					tuple = greaterThanTuple();
				else 
					tuple = equalTuple();
			}
			catch(Exception e){
				e.printStackTrace();
			}
			if(tuple == null) return null;
			if(evaluateTuple(tuple)){
				break;
			}
			}
			return tuple;
		}
		 else {
				if (inputScan == null) return null;
				String line = null;
				Datum[] ret = null;
				while(true){
					try {
						line = inputScan.readLine();
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
			if(inputScan != null){
				inputScan.close();
			}
			inputScan = new BufferedReader(new FileReader(f));
		} catch (IOException e) {
			e.printStackTrace();
			inputScan = null;
		}
	}

	@Override
	public ColumnInfo[] getSchema() {
		return schema;
	}

	public Datum[] equalTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				if(map.containsKey(eval.getValue())){
					ArrayList<String[]> tupleList = (ArrayList<String[]>) map.get(eval.getValue());
					iter = tupleList.iterator();
					while(iter.hasNext()) {
						String[] obj = iter.next();
						Datum[] tuples = toDatum(obj);
						return tuples;
					}
					iter=null;
					exitCondition = true;
				}
				else {
					exitCondition = true;
				}
			}
			else {
				while(iter.hasNext()) {
					String[] obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
				exitCondition = true;
			}
		}
		return null;
	}
	
	public Datum[] lessThanTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				ArrayList<String[]> tupleList = (ArrayList<String[]>) map.get(iterList[where]);
				where--;
				iter = tupleList.iterator();
				while(iter.hasNext()) {
					String[] obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
			}
			else {
				while(iter.hasNext()) {
					String[] obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
				iter=null;
			}
			if(where<0)
				exitCondition = true;
		}
		return null;
	}
	
	
	public Datum[] greaterThanTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				ArrayList<String[]> tupleList = (ArrayList<String[]>) map.get(iterList[where]);
				iter = tupleList.iterator();
				while(iter.hasNext()) {
					String[] obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
			}
			else {
				while(iter.hasNext()) {
					String[] obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
				iter=null;
			}
			where++;
			if(where == iterList.length)
				exitCondition = true;
		}
		return null;
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
	
	public int searchKey(String value){
		ArrayList<String> list = new ArrayList(map.keySet());
		int i,start=0,end=list.size();
		i=(start+end)/2;
		int compare;
		while(i>start&&i<end){
			compare = value.compareTo(list.get(i));
			if(compare==0)
				return i;
			else if(compare<0)
				end = i;
			else 
				start = i;
			i=(start+end)/2;
			if(i==start||i==end)
				return i;
		}
		return i;
	}
	
	public Datum[] toDatum(String[] tuple){
		Datum[] d = new Datum[tuple.length];
		int i=0;
		for(String s: tuple){
			d[i] = new Datum(s);
			i++;
		}
		return d;
	}
	
}
