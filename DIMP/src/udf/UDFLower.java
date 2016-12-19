package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class UDFLower extends UDF {
	public Text evaluate(final Text s){
        if (null == s){
            return null;
        }
        return new Text(s.toString().toLowerCase());
    }
}
