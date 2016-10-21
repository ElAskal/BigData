package bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements WritableComparable<Object> {
	
	public boolean isCity;
	public String region;
	public String country;
	
	
	public TaggedKey(){}
	
	public TaggedKey(String r , boolean city, String name)
	{
		this.isCity = city;
		this.region = r;
		this.country = name;
	}

	
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isCity);
		out.writeUTF(country);
		out.writeUTF(region);
	}

	public void readFields(DataInput in) throws IOException {
		isCity = in.readBoolean();
		country = in.readUTF();
		region = in.readUTF();
	}

	public int compareTo(Object o) {
		TaggedKey tmp = (TaggedKey) o;
		int res = country.compareTo(tmp.country);
		if (res != 0)
		{
				return res;
		}
		res = region.compareTo(tmp.region);
		if (res != 0)
		{
			return res;
		}		
		if ((isCity && tmp.isCity) || (!isCity && !tmp.isCity))
		{
			return 0;
		}
		if (isCity)
		{
			return -1;
		}
		else
		{
			return 1;
		}

	}
	
	public int compareTo(TaggedKey t)
	{
		return get() - t.get();
	}

	public int get(){
		return region.hashCode() + country.hashCode();
	}
	
}
