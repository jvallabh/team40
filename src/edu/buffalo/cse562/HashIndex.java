package edu.buffalo.cse562;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class HashIndex {
	public ColumnInfo[] schema;
	Operator input;
	static HashMap tableIndex = new HashMap<String,int[]>();
	
	public HashIndex(Operator input) {
		this.input = input;
		schema = input.getSchema();
	}
	
	public static void buildTableIndex() {
        tableIndex.put("lineitem", new int[] { 0, 2, 8, 12, 10 });
        tableIndex.put("orders", new int[] { 0, 1, 4 });
        tableIndex.put("customer", new int[] { 0, 3, 6 });
        tableIndex.put("supplier", new int[] { 0, 3 });
        tableIndex.put("nation", new int[] { 0, 2 });
        tableIndex.put("part", new int[] { 0, 3, 5 });
        tableIndex.put("partsupp", new int[] { 0 });
        tableIndex.put("region", new int[] { 0, 1 });

	}
	
	public void buildIndex(int col) {
		SortableTuple.schema=schema;
		SortableTuple.orderByColumnIndex=new int[]{col};
		SortableTuple.orderIndex = new int[]{1};
		Datum[] tuple = input.readOneTuple();
		ArrayList<SortableTuple> sortableTuples = new ArrayList<SortableTuple>();
		LinkedHashMap<String, ArrayList<String[]>> hashIndex = new LinkedHashMap<String, ArrayList<String[]>>();
		while(tuple!=null){
			SortableTuple sortableTuple = new SortableTuple(tuple);
			sortableTuples.add(sortableTuple);
			tuple = input.readOneTuple();
		}
		Collections.sort(sortableTuples, new SortableTuple(null));
		for(SortableTuple sTuple: sortableTuples){
			if(hashIndex.containsKey(sTuple.tuple[col].element))
				hashIndex.get(sTuple.tuple[col].element).add(toStringFromDatum(sTuple.tuple));
			else {
				ArrayList<String[]> tuples = new ArrayList<String[]>();
				tuples.add(toStringFromDatum(sTuple.tuple));
				hashIndex.put(sTuple.tuple[col].element, tuples);	
			}
		}
		if(hashIndex.containsKey("1"))
			System.out.println("Yes I contain 1");
		System.out.println(hashIndex.size());
		try{
        File file = new File(Main.indexDir+schema[col].tableName+schema[col].colDef.getColumnName());
        FileOutputStream f = new FileOutputStream(file);
        ObjectOutputStream s = new ObjectOutputStream(f);
        s.writeObject(hashIndex);
        s.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	public String[] toStringFromDatum(Datum[] tuple){
		String[] s = new String[tuple.length];
		int i=0;
		for(Datum d:tuple){
			s[i] = d.element;
			i++;
		}
		return s;
	}

}
