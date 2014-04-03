package edu.buffalo.cse562;
import java.util.Comparator;

public class SortableTuple implements Comparator<SortableTuple> {
	
	Datum[] tuple;
	public static ColumnInfo[] schema;
	public static int[] orderByColumnIndex;
	public static int[] orderIndex;//Here we store ascending or descending order. 1 for ASC, -1 for DESC
	
	public SortableTuple(Datum[] tuple){
		this.tuple =tuple;
		
	}

	@Override
	public int compare(SortableTuple arg0, SortableTuple arg1) {
		// TODO Auto-generated method stub
		int  x=0;int j=0;
		for(int i:orderByColumnIndex){
			//System.out.println("Column datatype is: "+schema[i].colDef.getColDataType());
			String colDataType = schema[i].colDef.getColDataType().getDataType();
			if(x!=0) break;
			if(colDataType.equalsIgnoreCase("double")){
				x  = orderIndex[j]*Double.compare(Double.parseDouble(arg0.tuple[i].element),Double.parseDouble(arg1.tuple[i].element));
			}				 
			if(colDataType.equalsIgnoreCase("int")){
				x  = orderIndex[j]*Integer.compare(Integer.parseInt(arg0.tuple[i].element),Integer.parseInt(arg1.tuple[i].element));
			}				 
			/*if(colDataType.equalsIgnoreCase("double")){
				x  =orderIndex[j]*arg0.tuple[i].toString().compareTo(arg1.tuple[i].element);
			}*/				 
			if(colDataType.equalsIgnoreCase("string")){
				x  =orderIndex[j]*arg0.tuple[i].toString().compareTo(arg1.tuple[i].element);
			}				 
			if(colDataType.equalsIgnoreCase("date")){
				x  =orderIndex[j]*arg0.tuple[i].Date().compareTo(arg1.tuple[i].Date());
			}				 
			j++;
		}
		return x;
	}

}
