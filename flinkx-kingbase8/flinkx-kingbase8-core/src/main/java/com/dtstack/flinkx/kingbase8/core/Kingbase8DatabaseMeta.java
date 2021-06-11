package com.dtstack.flinkx.kingbase8.core;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.kingbase.KingbaseDatabaseMeta;

public class Kingbase8DatabaseMeta extends KingbaseDatabaseMeta {
	private static final long serialVersionUID = -2914980344554470979L;

	@Override
	public EDatabaseType getDatabaseType() {
		return EDatabaseType.Kingbase8;
	}

	@Override
	public String getDriverClass() {
		return "com.kingbase8.Driver";
	}
}
