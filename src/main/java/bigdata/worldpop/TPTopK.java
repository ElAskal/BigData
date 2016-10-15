
package bigdata.worldpop;

import org.apache.hadoop.util.ProgramDriver;

public class TPTopK {

	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("topk", bigdata.worldpop.Top.class, "creates a topK");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
	
}