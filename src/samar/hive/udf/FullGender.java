package samar.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * select gender from raw_adhar; M F M F T
 * 
 * select FullGender(gender) from raw_adhar; King Queen King Queen Others
 */
public class FullGender extends UDF {
	private Text result = new Text();

	public Text evaluate(Text str) {
		if (str == null) {
			result.set("empty");

		}
		if (str.toString().equalsIgnoreCase("F")) {
			result.set("Queen");
		} else if (str.toString().equalsIgnoreCase("M")) {
			result.set("King");
		} else {
			result.set("Unkown");
		}
		return result;
	}
}
