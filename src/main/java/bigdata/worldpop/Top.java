package bigdata.worldpop;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

  public class Top extends Configured implements Tool{
	  
	  public static class TopKMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		  public int k = 0;
		  
		  private TreeMap<LongWritable, Text> topKPop = new TreeMap<LongWritable, Text>();
		  
		  @Override
		  public void setup(Context context) {
			  Configuration conf = context.getConfiguration();
			  k = Integer.parseInt(conf.get("k"));
		  }
			  
		  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			  String s = new IntWritable(k).toString();
			  System.out.println(s);
				if (key.get() == 0 ) return;
				String tokens[] = value.toString().split(",");
				if (tokens.length < 7 || tokens[4].length()==0) return;
				int pop = 0;
				try {
					pop = Integer.parseInt(tokens[4]);
				}
				catch(Exception e) {				
				return;
				}
				topKPop.put(new LongWritable(pop), new Text(tokens[2]));
				if (topKPop.size() > k)
					topKPop.remove(topKPop.firstKey());
			}
		  
		  protected void cleanup(Context context) throws IOException, InterruptedException {
			  for (Text c : topKPop.values()) {
			  context.write(NullWritable.get(), c);
			  }
		  }
	  }
	 
		public static class TopKReducer extends Reducer<NullWritable, LongWritable, IntWritable, Text> {
			  public int k = 0;
			  private TreeMap<IntWritable, Text> topKPop = new TreeMap<IntWritable, Text>();
			  public void setup(Context context) {
				  Configuration conf = context.getConfiguration();
				  k = Integer.parseInt(conf.get("k"));
			  	}
			  
			  public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException,
			  				InterruptedException {
				  int cpt = 1;
		/*		  for (Text value : values) {
					  topKPop.put(new IntWritable(cpt), value);
					  if (topKPop.size() > k)
						  topKPop.remove(topKPop.firstKey());
					  cpt++;
				  }*/
				  cpt = 1;
				  for (Text c : topKPop.descendingMap().values()) {
					  context.write(new IntWritable(cpt), c);
					  cpt++;
				  }
			  }
		}
		  			
	  public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "TopK");
	    try {
	    	job.getConfiguration().set("k", args[0]);
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path("Result"));
	    }
	    catch (Exception e)
	    {
	    	System.out.println(" bad arguments, waiting for 2 arguments [Integer] [inputURI]");
	    }
	    job.setNumReduceTasks(1);
	    job.setJarByClass(Top.class);
	    job.setMapperClass(TopKMapper.class);
	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(TopKReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	  }
	  
	  public static void main(String args[]) throws Exception {
			System.exit(ToolRunner.run(new Top(), args));
	  }
  }
  
	  
