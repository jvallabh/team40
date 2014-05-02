package edu.buffalo.cse562;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;

public class BuildIndex {
	PrimaryTreeMap<String, ArrayList<String>> tree;
	Operator input;
	int index;
	ColumnInfo[] schema;
	RecordManager indexFile;
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
	    tableIndex.put("supplier", new int[] { 3 });
	    tableIndex.put("nation", new int[] { 0, 2 });
		tableIndex.put("part", new int[] { 3, 5 });
		tableIndex.put("partsupp", new int[] { 0 });
	}
	
	public void buildIndex(int col) throws Exception{
		this.schema = input.getSchema();
		int check=1000;
		SortableTuple.schema=schema;
		SortableTuple.orderByColumnIndex=new int[]{col};
		SortableTuple.orderIndex = new int[]{1};
		Datum[] tuple1 = input.readOneTuple();
		ArrayList<SortableTuple> sortableTuples = new ArrayList<SortableTuple>();
		LinkedHashMap<String, ArrayList<String>> hashIndex = new LinkedHashMap<String, ArrayList<String>>();
		while(tuple1!=null){
			SortableTuple sortableTuple = new SortableTuple(tuple1);
			sortableTuples.add(sortableTuple);
			tuple1 = input.readOneTuple();
		}
		Collections.sort(sortableTuples,new SortableTuple(null));
		tree = indexFile.treeMap(this.schema[col].tableName+"_"+this.schema[col].colDef.getColumnName());
        for(SortableTuple sTuple: sortableTuples){
                if(hashIndex.containsKey(sTuple.tuple[col].element))
                        hashIndex.get(sTuple.tuple[col].element).add(toDatumString(toStringFromDatum(sTuple.tuple)));
                else {
                        ArrayList<String> tuples = new ArrayList<String>();
                        tuples.add(toDatumString(toStringFromDatum(sTuple.tuple)));
                        hashIndex.put(sTuple.tuple[col].element, tuples);        
                }
        }
        Iterator<Entry<String, ArrayList<String>>> mapIter = hashIndex.entrySet().iterator();
        while(mapIter.hasNext()){
                Entry<String, ArrayList<String>> currEntry = mapIter.next();
                tree.put(currEntry.getKey(), currEntry.getValue());
                if(check==0){
                        indexFile.commit();
                        check=1000;
                }
                else
                        check--;                        
        }
		/*tree = indexFile.treeMap(this.schema[col].tableName+"_"+this.schema[col].colDef.getColumnName());
		for(SortableTuple sTuple: sortableTuples){
			if(tree.containsKey(sTuple.tuple[col].element))
				tree.get(sTuple.tuple[col].element).add(toDatumString(toStringFromDatum(sTuple.tuple)));
			else {
				ArrayList<String> tuples = new ArrayList<String>();
				tuples.add(toDatumString(toStringFromDatum(sTuple.tuple)));
				tree.put(sTuple.tuple[col].element, tuples);	
			}
			if(check==0){
				indexFile.commit();
				check=1000;
			}
			else
				check--;
		}*/
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
}