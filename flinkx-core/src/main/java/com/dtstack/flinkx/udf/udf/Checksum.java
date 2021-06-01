package com.dtstack.flinkx.udf.udf;

import com.dtstack.flinkx.udf.UDF;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.zip.CRC32;

@UDF(name = "CHECKSUM")
public class Checksum extends ScalarFunction {

    /**
     * 求校验和
     * @param string
     * @return
     */
    public String eval(String string) {
        byte[] data = string.getBytes();
        CRC32 crc = new CRC32();
        crc.update(data);
        return String.valueOf(crc.getValue());
    }

}
