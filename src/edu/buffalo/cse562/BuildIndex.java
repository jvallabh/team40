package edu.buffalo.cse562;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import jdbm.*;
import jdbm.btree.BTree;

public class BuildIndex {
	BTree tree;
	Operator input;
	int index;
	ColumnInfo[] schema;
	static RecordManager indexFile;
	HashMap<String, int[]> tableIndex = new HashMap<String, int[]>();
	HashMap<String, ArrayList<Datum[]>> map = new HashMap<String, ArrayList<Datum[]>>();

	public BuildIndex(Operator input, int index) {
		this.input = input;
		this.index = index;
	}

	public void buildTableIndex() {
		tableIndex.put("lineitem", new int[] { 8, 12, 10 });
		tableIndex.put("orders", new int[] { 4 });
		tableIndex.put("customer", new int[] { 6 });
	//	tableIndex.put("supplier", new int[] { 3 });
	//	tableIndex.put("nation", new int[] { 0, 2 });
	//	tableIndex.put("part", new int[] { 3, 5 });
	//	tableIndex.put("partsupp", new int[] { 0 });
		tableIndex.put("region", new int[] { 1 });
	}

	public void buildIndex() throws Exception {
		this.schema = input.getSchema();
		long recid;
		String type;
		for (int col : tableIndex.get(this.schema[0].tableName.toLowerCase())) {
			type = this.schema[col].colDef.getColDataType().toString();
			recid = indexFile.getNamedObject(this.schema[col].tableName + "_"+ this.schema[col].colDef.getColumnName());
			if (recid != 0) {
				tree = BTree.load(indexFile, recid);
			} else {
				tree = BTree.createInstance(indexFile, new ExampleComparator(type));
				indexFile.setNamedObject(this.schema[col].tableName + "_"+ this.schema[col].colDef.getColumnName(),tree.getRecid());
			}
			buildHashByColumn(col);
			int i = 1000;
			Iterator<Entry<String, ArrayList<Datum[]>>> iterator = map.entrySet().iterator();
			Entry<String, ArrayList<Datum[]>> iter;
			while (iterator.hasNext()) {
				iter = iterator.next();
				tree.insert(new Datum(iter.getKey()), iter.getValue(), false);
				if (i == 0) {
					i = 1000;
					indexFile.commit();
				} else
					i--;
			}
			input.reset();
			indexFile.commit();
			System.out.println(tree.size());
		}
	}
	
	public void buildHashByColumn(int col){
		Datum[] tuple = input.readOneTuple();
		//ArrayList<Datum[]> tupleList;
		map = new HashMap<String, ArrayList<Datum[]>>();
		while (tuple != null) {
			if (map.containsKey(tuple[col].element)) {
				map.get(tuple[col].element).add(tuple);

			} else {
				// Create new list
				ArrayList<Datum[]> tuples = new ArrayList<Datum[]>();
				tuples.add(tuple);
				map.put(tuple[col].element, tuples);
			}
			tuple = input.readOneTuple();
		}
		
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
