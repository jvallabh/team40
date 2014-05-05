package edu.buffalo.cse562;

import java.io.IOException;
import java.util.ArrayList;

import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class JustDatum implements Serializer<Datum>{

	public Datum deserialize(SerializerInput arg0) throws IOException{
		Datum d;
		d = new Datum(arg0.readUTF());
		return d;
	}
	
	public void serialize(SerializerOutput arg0, Datum d) throws IOException {
		arg0.writeUTF(d.element);
	}
}
