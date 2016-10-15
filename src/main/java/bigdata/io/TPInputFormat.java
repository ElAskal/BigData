/**
 * @author David Auber 
 * @date 07/10/2016
 * Ma�tre de conf�rencces HDR
 * LaBRI: Universit� de Bordeaux
 */
package bigdata.io;

import org.apache.hadoop.util.ProgramDriver;

public class TPInputFormat {

	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("generator", bigdata.io.PointsGenerator.class, "a map/reduce program that generates random points into a file");
			pgd.addClass("pi", bigdata.io.Pi.class, "a map/reduce program that computes Pi using Montecarlo algorithm");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
