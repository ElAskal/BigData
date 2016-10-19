 package bigdata;

import org.apache.hadoop.util.ProgramDriver;

public class BigData {
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("hdfs", bigdata.hdfs.TPHdfs.class, "use hdfs API");
			pgd.addClass("worldpop", bigdata.worldpop.TPWorldPopulation.class, "filter/resume worlpopulation file");
			pgd.addClass("IOFormat", bigdata.io.TPInputFormat.class, "Create a random point2D import format");
			pgd.addClass("topK", bigdata.worldpop.TPTopK.class, "Create a top 10");
			pgd.addClass("RSJoin", bigdata.worldpop.RSJoin.class, "Create a RS join of two files");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
