package bigdata.worldpop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bigdata.io.TaggedValue;

public class RSJoin extends Configured implements Tool{
	
	/* Code : tokens[0]wcp = tokens[0]rc, Numéro : tokens[3]wcp = tokens[1]rc, Nom : tokens[2]wcp = tokens[2]rc */
	public static class RSJoinWCPMapper extends Mapper<LongWritable, Text, Text, TaggedValue>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
			TaggedValue tv = new TaggedValue(true, tokens[2]);
			context.write(new Text(tokens[0]+","+tokens[3]), tv);
			
		}
	}
	
	public static class RSJoinRCMapper extends Mapper<LongWritable, Text, Text, TaggedValue>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
			TaggedValue tv = new TaggedValue(false, tokens[2]);
			context.write(new Text(tokens[0]+","+tokens[1]), tv);
		}
	}
	
	public static class RSJoinReducer extends Reducer<Text, TaggedValue, Text, Text>{
		public void reduce(Text key, Iterable<TaggedValue> values, Context context) throws IOException,
			InterruptedException {
			List<String> lc = new ArrayList<String>();
			List<String> lr = new ArrayList<String>();
			for(TaggedValue value : values){
				if(value.isCity == true){
					lc.add(value.name);
				}
				else
					lr.add(value.name);
			}
		}
	}
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "RSJoin");
	    Path pOut = new Path("Result");
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RSJoinWCPMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RSJoinRCMapper.class);
		FileOutputFormat.setOutputPath(job, pOut);
	    job.setNumReduceTasks(1);
	    job.setJarByClass(Top.class);
	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(TaggedValue.class);
	    job.setReducerClass(RSJoinReducer.class);
	    job.setOutputKeyClass(Text.class);
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
			System.exit(ToolRunner.run(new RSJoin(), args));
	  }

}
