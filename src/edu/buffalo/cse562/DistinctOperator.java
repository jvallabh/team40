package edu.buffalo.cse562;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;


public class DistinctOperator implements Operator {

	Operator input;
	List<SelectItem> selectItems;
	LinkedHashMap<String, Datum[]> distinctMap = new LinkedHashMap<String, Datum[]>();
	ColumnInfo[] schema;
	ArrayList datumList;
	
	public DistinctOperator(Operator input, List selectItems){
		this.input = input;
		this.selectItems = selectItems;
		this.schema = input.getSchema();
		doDistinct();
	}
	
	public void doDistinct() {
		Datum[] d;
		StringBuilder sb = new StringBuilder();
		List<Integer> columnIds = getColumnIds();
		while((d=input.readOneTuple())!=null){
			for(int i:columnIds) {
				sb.append(d[i]);
			}
			if (distinctMap.containsKey(sb.toString())==false)
				distinctMap.put(sb.toString(), d);
			sb.delete(0, sb.length());
		}
		datumList = new ArrayList<Object> (Arrays.asList(distinctMap.values().toArray()));
	}
	
	public List getColumnIds() {
		List columnIds = new ArrayList<>();
		SelectExpressionItem exp;
		Column col;
		for(int i=0;i<selectItems.size();i++){
			exp = (SelectExpressionItem)selectItems.get(i);
			col = (Column) exp.getExpression();
			for (int j=0; i<schema.length; i++) {
				if (schema[j].colDef.getColumnName().equalsIgnoreCase(col.getColumnName())) {
					columnIds.add(j);
					break;
				}
			}
		}
		return columnIds;
	}
	
	@Override
	public Datum[] readOneTuple() {
		Datum[] d=null;
		if(datumList.isEmpty()==false)
		d = (Datum[])datumList.get(0);
		if(d!=null) {
			datumList.remove(0);
			return d;
		}
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ColumnInfo[] getSchema() {
		return input.getSchema();
	}

}
