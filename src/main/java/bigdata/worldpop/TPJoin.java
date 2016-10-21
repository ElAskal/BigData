
package bigdata.worldpop;

import org.apache.hadoop.util.ProgramDriver;

public class TPJoin{

	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("RS", bigdata.worldpop.RSJoin.class, "RS Join part");
			pgd.addClass("Scale", bigdata.worldpop.ScalJoin.class, "Scale Join part");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
	
}