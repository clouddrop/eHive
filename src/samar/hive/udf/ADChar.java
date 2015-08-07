package samar.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ADChar extends UDF {
	private Text result = new Text();

	public Text evaluate(Text str) {
		if (str == null) {
			result.set("empty");

		} else {
			result.set(str + "end");
		}
		return result;
	}
}
