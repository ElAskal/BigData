/**
 * @author David Auber 
 * @date 07/10/2016
 * Ma�tre de conf�rencces HDR
 * LaBRI: Universit� de Bordeaux
 */
package bigdata.worldpop;

import org.apache.hadoop.util.ProgramDriver;

public class TPWorldPopulation {

	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("filter", bigdata.worldpop.FilterCities.class, "filter cities");
			pgd.addClass("resume", bigdata.worldpop.ResumeCities.class, "Agregate cities according to their population");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
	
}