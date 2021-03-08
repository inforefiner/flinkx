package com.dtstack.flinkx.udf.udf;

import com.dtstack.flinkx.udf.UDF;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Created by P0007 on 2020/4/3.
 * 字符串截取自定义函数
 */
@UDF(name = "SUBSTRING_LOCALE")
public class StringUDF extends ScalarFunction {

    /**
     * 按separator截取string，并返回第num个元素
     * @param string
     * @return
     */
    public String eval(String string, String separator, int num) {
        int count = 0;
        int nextIndex = 0;
        int fromIndex = 0;
        int subStrIndex = 0;
        while (count != num) {
            subStrIndex = fromIndex;
            nextIndex = string.indexOf(separator, fromIndex);
            fromIndex = nextIndex + 1;
            count++;
        }
        String res = string.substring(subStrIndex, nextIndex);
        return res;
    }

}
