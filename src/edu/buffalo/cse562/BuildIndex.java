package edu.buffalo.cse562;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;

import jdbm.*;
import jdbm.btree.BTree;

public class BuildIndex {
	PrimaryTreeMap<String, ArrayList<String>> tree;
	Operator input;
	int index;
	ColumnInfo[] schema;
	static RecordManager indexFile;
	HashMap<String,int[]> tableIndex = new HashMap<String,int[]>();
	HashMap<String,ArrayList<String>> tmp = new HashMap<String,ArrayList<String>>();
	
	public BuildIndex(Operator input,int index) {
		this.input = input;
		this.index = index;
	}
	
	public void buildTableIndex(){
		tableIndex.put("lineitem", new int[] { 8, 12, 10 });
		tableIndex.put("orders", new int[] { 4 });
		tableIndex.put("customer", new int[] { 6 });
		tableIndex.put("region", new int[] { 1 });
	//	tableIndex.put("supplier", new int[] { 3 });
	//	tableIndex.put("nation", new int[] { 0, 2 });
	//	tableIndex.put("part", new int[] { 3, 5 });
	//	tableIndex.put("partsupp", new int[] { 0 });
	}
	
	public void buildIndex(int col) throws Exception{
		this.schema = input.getSchema();
		long recid,check=1000;
		String type;
		SortableTuple.schema=schema;
		SortableTuple.orderByColumnIndex=new int[]{col};
		SortableTuple.orderIndex = new int[]{1};
		Datum[] tuple1 = input.readOneTuple();
		ArrayList<SortableTuple> sortableTuples = new ArrayList<SortableTuple>();
		LinkedHashMap<String, ArrayList<String[]>> hashIndex = new LinkedHashMap<String, ArrayList<String[]>>();
		while(tuple1!=null){
			SortableTuple sortableTuple = new SortableTuple(tuple1);
			sortableTuples.add(sortableTuple);
			tuple1 = input.readOneTuple();
		}
		Collections.sort(sortableTuples,new SortableTuple(null));
		tree = indexFile.treeMap(this.schema[col].tableName+"_"+this.schema[col].colDef.getColumnName());
		for(SortableTuple sTuple: sortableTuples){
			if(tree.containsKey(sTuple.tuple[col].element))
				tree.get(sTuple.tuple[col].element).add(toDatumString(toStringFromDatum(sTuple.tuple)));
			else {
				ArrayList<String> tuples = new ArrayList<String>();
				tuples.add(toDatumString(toStringFromDatum(sTuple.tuple)));
				tree.put(sTuple.tuple[col].element, tuples);	
			}
		}
			if(check==0){
				indexFile.commit();
				check=1000;
			}
			else
				check--;
		//type = this.schema[col].colDef.getColDataType().toString();
	/*	recid = indexFile.getNamedObject(this.schema[col].tableName+"_"+this.schema[col].colDef.getColumnName());
		if(recid != 0) {
			tree = BTree.load(indexFile, recid);
		}
		else {
			tree = BTree.createInstance(indexFile, new ExampleComparator(type));
			indexFile.setNamedObject(this.schema[col].tableName+"_"+this.schema[col].colDef.getColumnName(), tree.getRecid());
		}*/
	/*	String[] tuple = toStringFromDatum(input.readOneTuple());
		int i=1000;
		ArrayList<String[]> tupleList;
		while(tuple!=null) {
			tupleList = (ArrayList<String[]>)tmp.get(tuple[col]);
			if(tupleList==null)
				tupleList = new ArrayList<>();
			tupleList.add(tuple);
			tmp.put(tuple[col], tupleList);
			tuple = toStringFromDatum(input.readOneTuple());
		}
		*/
		input.reset();
		indexFile.commit();
		indexFile.clearCache();
		System.out.println(tree.size());
	}
	
	
	public String[] toStringFromDatum(Datum[] tuple){
		if(tuple == null)
			return null;
		String[] s = new String[tuple.length];
		int i=0;
		for(Datum d:tuple){
			s[i] = d.toString();
			i++;
		}
		return s;
	}
	public String toDatumString(String[] tuple) {
		StringBuilder datumToString = new StringBuilder();
		for(String d:tuple){
			datumToString.append("|"+d.toString());
		}
		return datumToString.substring(1);
	}
	
	public static class ExampleComparator implements Serializable,
	Comparator<Datum> {
String type;
int x;

	public ExampleComparator(String type) {
		this.type = type;
	}

	public int compare(Datum obj1, Datum obj2) {
	
		if (type.equalsIgnoreCase("int"))
			x = Integer.compare(Integer.parseInt(obj1.element),
					Integer.parseInt(obj2.element));
		else if (type.equalsIgnoreCase("DECIMAL"))
			x = Double.compare(Double.parseDouble(obj1.element),
					Double.parseDouble(obj2.element));
		else if (type.equalsIgnoreCase("double"))
			x = Double.compare(Double.parseDouble(obj1.element),
					Double.parseDouble(obj2.element));
		else if (type.equalsIgnoreCase("string")
				|| type.equalsIgnoreCase("VARCHAR")
				|| type.equalsIgnoreCase("CHAR"))
			x = obj1.toString().compareTo(obj2.toString());
		else if (type.equalsIgnoreCase("DATE"))
			x = obj1.Date().compareTo(obj2.Date());
		return x;
	}
	
	}
}