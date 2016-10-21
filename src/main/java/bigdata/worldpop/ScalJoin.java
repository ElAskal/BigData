package bigdata.worldpop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bigdata.io.TaggedKey;
import bigdata.io.TaggedValue;

public class ScalJoin extends Configured implements Tool{
	
	/* Code : tokens[0]wcp = tokens[0]rc, Numéro : tokens[3]wcp = tokens[1]rc, Nom : tokens[2]wcp = tokens[2]rc */
	
	public static class ScalJoinWCPMapper extends Mapper<LongWritable, Text, TaggedKey, TaggedValue>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
			TaggedKey tk = new TaggedKey(tokens[3], true, tokens[0].toLowerCase() + ",");
			context.write(tk, new TaggedValue(true, tokens[1]));
			
		}
	}
	
	public static class ScalJoinRCMapper extends Mapper<LongWritable, Text, TaggedKey, TaggedValue>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
			TaggedKey tk = new TaggedKey(tokens[1], false, tokens[0].toLowerCase() + ",");
			context.write(tk, new TaggedValue(false, tokens[2]));
		}
	}
	
	public static class JoinPartitioner extends Partitioner<TaggedKey, TaggedValue>
	{

		public int getPartition(TaggedKey arg0, TaggedValue arg1, int arg2) {
				return arg0.get() % arg2;	
		}		
	}
	
	public static class NaturalWritableComparator extends WritableComparator
	{
		public NaturalWritableComparator()
		{
			super(bigdata.io.TaggedKey.class, true);
		}
		
		public static int compare(TaggedKey o1, TaggedKey o2)
		{
			return o1.compareTo((TaggedKey) o2);
		}
	}
	
	public static class DataWritableComparator extends WritableComparator
	{
		public DataWritableComparator()
		{
			super(bigdata.io.TaggedKey.class, true);
		}		

		public int compare(TaggedKey o1, TaggedKey o2)
		{
			return o1.compareTo((Object) o2);
		}
	}
	
	public static class ScalJoinReducer extends Reducer<TaggedKey, TaggedValue, NullWritable, Text>{
		public void reduce(TaggedKey key, Iterable<TaggedValue> values, Context context) throws IOException,
		InterruptedException {
		Text lc = new Text();
		Text lr = new Text();
		for(TaggedValue value : values){
			if(value.getCity()){
				lc.set(value.name);
			}
			else 
			{
				lr.set(value.name);
				String s = lr.toString() + ", " + lc.toString();
				context.write(NullWritable.get(), new Text(s));	
			}
		if (lr.toString().isEmpty() || lc.toString().isEmpty())
			return;
		}
	}
	}
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "RSJoin");
	    Path pOut = new Path("Result");
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ScalJoinWCPMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ScalJoinRCMapper.class);
		FileOutputFormat.setOutputPath(job, pOut);
	    job.setNumReduceTasks(1);
	    job.setJarByClass(Top.class);
	    job.setMapOutputKeyClass(TaggedKey.class);
	    job.setMapOutputValueClass(TaggedValue.class);
	    job.setPartitionerClass(JoinPartitioner.class);
	    job.setGroupingComparatorClass(NaturalWritableComparator.class);
	    job.setSortComparatorClass(DataWritableComparator.class);
	    job.setReducerClass(ScalJoinReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    int jobComplete = 1;
	    jobComplete = job.waitForCompletion(true) ? 0 : 1;
	    if(job.isSuccessful()){
	    	FileSystem fs = FileSystem.get(conf);
	    	Path p = new Path(FileOutputFormat.getOutputPath(job)+"/part-r-00000");
	    	FileStatus[] fss = fs.listStatus(p);
	        for (FileStatus status : fss) {
	            Path path = status.getPath();
	            //SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
	            FSDataInputStream inputStream = fs.open(path);
	            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
	            String record;
	            System.out.println("\tResults");
	            while((record = reader.readLine()) != null) {
	                System.out.println("\t\t"+record);
	            }
	            reader.close();
	        }
	    }
	    return jobComplete;
	  }
	
	  public static void main(String args[]) throws Exception {
			System.exit(ToolRunner.run(new ScalJoin(), args));
	  }

}
