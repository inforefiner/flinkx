package com.dtstack.flinkx.udf.udtf;

import com.dtstack.flinkx.udf.UDF;
import org.apache.flink.table.functions.TableFunction;

/**
 * 行转列
 * 输入字段col1,col2,col3
 * 输入数据:
 *       v1,v21;v22;v23,v3
 * 输出数据:
 *       v1,v21,v3
 *       v1,v22,v3
 *       v1,v23,v3
 *
 */
@UDF(name = "EXPLODE")
public class ExplodeUDTF extends TableFunction<String> {

    public void eval(String column, String separator) {
        String[] strings = column.split(separator);
        for (String str : strings) {
            collect(str);
        }
    }

}
