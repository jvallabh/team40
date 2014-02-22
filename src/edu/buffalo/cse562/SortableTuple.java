package edu.buffalo.cse562;
import java.util.Comparator;

public class SortableTuple implements Comparator<SortableTuple> {
	
	private Datum[] tuple;
	public static ColumnInfo[] schema;
	public static int[] orderby;
	
	public SortableTuple(Datum[] tuple, ColumnInfo[] schema){
		this.tuple =tuple;
		this.schema= schema;
		
	}

	@Override
	public int compare(SortableTuple arg0, SortableTuple arg1) {
		// TODO Auto-generated method stub
		int  x=0;
		for(int i:orderby){
			if(x!=0) break;
			if(schema[i].colDef.getColDataType().equals("double"))
				 x  = Double.compare(Double.parseDouble(arg0.tuple[i].element),Double.parseDouble(arg1.tuple[i].element));
			if(schema[i].colDef.getColDataType().equals("int"))
				 x  = Integer.compare(Integer.parseInt(arg0.tuple[i].element),Integer.parseInt(arg1.tuple[i].element));
			if(schema[i].colDef.getColDataType().equals("double"))
				 x  =arg0.tuple[i].toString().compareTo(arg1.tuple[i].element);
		}
		return x;
	}

}
