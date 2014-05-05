package edu.buffalo.cse562;

import java.io.IOException;
import java.util.ArrayList;

import jdbm.*;

public class DatumSerializer implements Serializer<ArrayList <Datum[]>> {
	int listSize;
	int schemaLength;
	
	public DatumSerializer(int length) {
		this.schemaLength = length;
	}

	@Override
	public ArrayList<Datum[]> deserialize(SerializerInput arg0) throws IOException,
			ClassNotFoundException {
		listSize = arg0.readInt();
		ArrayList<Datum[]> datumList = new ArrayList<Datum[]>();
		for(int j=0;j<listSize;j++){
			Datum[] datum = new Datum[schemaLength];
			datum = toDatum(arg0.readUTF());
			datumList.add(datum);
		}
		return datumList;
	}

	@Override
	public void serialize(SerializerOutput arg0, ArrayList<Datum[]> datumList) throws IOException {
		
		listSize = datumList.size();
		arg0.writeInt(listSize);
		String s;
		for(int j=0;j<listSize;j++){
				arg0.writeUTF(toDatumString(toStringFromDatum(datumList.get(j))));
		}
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
	
	public Datum[] toDatum(String line){
		String[] cols = line.split("\\|");
		Datum[] ret = new Datum[cols.length];
		for (int i=0; i<cols.length; i++) {
			//ret[i] = new Datum.Long(cols[i]);
			ret[i] = new Datum(cols[i]);
		}
		return ret;
	}

}
