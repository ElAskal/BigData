package bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TaggedValue implements Writable{
	public boolean isCity;
	public String name;
	
	public TaggedValue(){
	}
	
	public TaggedValue(boolean isCity, String name){
		this.isCity = isCity;
		this.name = name;
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isCity);
		out.writeBytes(name);
		
	}

	public void readFields(DataInput in) throws IOException {
		isCity = in.readBoolean();
		name = in.readLine();
		
	}
	public int getCity()
	{
		if (isCity)
			return 0;
		else
			return 1;
	}
}
