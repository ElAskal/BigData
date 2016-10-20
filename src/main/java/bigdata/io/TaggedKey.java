package bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements WritableComparable {
	
	public TaggedValue naturalKey;
	public String dataKey;
	
	
	public TaggedKey(){}
	
	public TaggedKey(String data, Boolean city, String name)
	{
		this.dataKey = data;
		this.naturalKey = new TaggedValue(city, name);
	}
	public void write(DataOutput out) throws IOException {
		out.writeBytes(dataKey);
		out.writeBoolean(naturalKey.isCity);
		out.writeBytes(naturalKey.name);		
	}

	public void readFields(DataInput in) throws IOException {
		dataKey = in.readLine();
		naturalKey.isCity = in.readBoolean();
		naturalKey.name = in.readLine();
	}

	public int compareTo(Object o) {
		if (o instanceof TaggedKey)
		{
			if (naturalKey.equals(((TaggedKey) o).naturalKey) && dataKey.equals(((TaggedKey) o).dataKey))
			{
				return 0;
			}
		}
		return 1;
	}

}
