package bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements WritableComparable {
	
	public TaggedValue naturalKey;
	public int dataKey;
	
	
	public TaggedKey(){}
	
	public TaggedKey(int data, Boolean city, String name)
	{
		this.dataKey = data;
		this.naturalKey = new TaggedValue(city, name);
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(dataKey);
		out.writeBoolean(naturalKey.isCity);
		out.writeBytes(naturalKey.name);		
	}

	public void readFields(DataInput in) throws IOException {
		dataKey = in.readInt();
		naturalKey.isCity = in.readBoolean();
		naturalKey.name = in.readLine();
	}

	public int compareTo(Object o) {
		if (o instanceof TaggedKey)
		{
			TaggedKey tmp = (TaggedKey) o;
			if (naturalKey.equals(tmp.naturalKey) && (dataKey == tmp.dataKey))
			{
				return 0;
			}
		}
		return 1;
	}

}
