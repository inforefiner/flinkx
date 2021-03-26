package com.dtstack.flinkx.udf.udtf;

import com.dtstack.flinkx.config.DimensionConfig;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.udf.plugin.jdbc.JDBCDialect;
import com.dtstack.flinkx.udf.plugin.jdbc.JDBCDialects;
import com.dtstack.flinkx.udf.plugin.jdbc.JDBCTypeUtil;
import com.dtstack.flinkx.udf.plugin.jdbc.JDBCUtils;
import com.dtstack.flinkx.udf.plugin.redis.config.FlinkJedisConfigAdapterBuilder;
import com.dtstack.flinkx.udf.util.ClassUtil;
import com.dtstack.flinkx.udf.util.RecordUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.dtstack.flinkx.udf.plugin.jdbc.JDBCUtils.getFieldFromResultSet;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link TableFunction} to query fields from JDBC by keys.
 * The query template like:
 * <PRE>
 * SELECT c, d, e, f from T where a = ? and b = ?
 * </PRE>
 *
 * <p>Support cache the result to avoid frequent accessing to remote databases.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 */
public class JDBCLookup extends TableFunction<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(JDBCLookup.class);
	private static final long serialVersionUID = 1L;

	private final String query;
	private final String drivername;
	private final String dbURL;
	private final String username;
	private final String password;
	private final TypeInformation[] keyTypes;
	private final int[] keySqlTypes;
	private final String[] fieldNames;
	private final TypeInformation[] fieldTypes;
	private final int[] outputSqlTypes;
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;

	private transient Connection dbConn;
	private transient PreparedStatement statement;
	private transient Cache<Row, List<Row>> cache;

	public JDBCLookup(DimensionConfig dimensionConfig) {
		String keyColumn = dimensionConfig.getParameter().getStringVal("keyColumn");
		String url = dimensionConfig.getParameter().getStringVal("url");
		String username = dimensionConfig.getParameter().getStringVal("username");
		String _password = dimensionConfig.getParameter().getStringVal("password");
		String password = StringUtils.isEmpty(_password) ? null : _password;
		String table = dimensionConfig.getParameter().getStringVal("table");
		String driverName = dimensionConfig.getParameter().getStringVal("driver");

		long cacheMaxSize = dimensionConfig.getParameter().getLongVal("cacheMaxSize", -1);
		long cacheExpireMs =  dimensionConfig.getParameter().getLongVal("cacheExpireMs", -1);
		int maxRetryTimes = dimensionConfig.getParameter().getIntVal("maxRetryTimes", 3);

		List columnList = dimensionConfig.getParameter().getColumn();
		List<MetaColumn> metaColumns = MetaColumn.getMetaColumns(columnList, false);
		String[] fieldNames = metaColumns.stream().map(metaColumn -> metaColumn.getName()).toArray(String[]::new);
		String[] fieldTypes = metaColumns.stream().map(metaColumn -> metaColumn.getType()).toArray(String[]::new);

		String[] keyCols = keyColumn.split(",");

		this.drivername = driverName;
		this.dbURL = url;
		this.username = username;
		this.password = password;
		this.fieldNames = fieldNames;
		this.fieldTypes = ClassUtil.toTypeInformations(fieldTypes);
		List<String> nameList = Arrays.asList(fieldNames);
		this.keyTypes = Arrays.stream(keyCols)
				.map(s -> {
					checkArgument(nameList.contains(s),
							"keyName %s can't find in fieldNames %s.", s, nameList);
					return this.fieldTypes[nameList.indexOf(s)];
				})
				.toArray(TypeInformation[]::new);

		JDBCDialect jdbcDialect = JDBCDialects.get(url).get();

		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
		this.keySqlTypes = Arrays.stream(keyTypes).mapToInt(JDBCTypeUtil::typeInformationToSqlType).toArray();
		this.outputSqlTypes = Arrays.stream(this.fieldTypes).mapToInt(JDBCTypeUtil::typeInformationToSqlType).toArray();
		this.query = jdbcDialect.getSelectFromStatement(
				table, fieldNames, keyCols);
		LOG.info("jdbc lookup query {}", this.query);
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		try {
			establishConnection();
			statement = dbConn.prepareStatement(query);
			this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
					.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
					.maximumSize(cacheMaxSize)
					.build();
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}
	}

	public void eval(Object... keys) {
		LOG.debug("lookup key {}", StringUtils.join(keys, ","));
		Row keyRow = Row.of(keys);
		if (cache != null) {
			List<Row> cachedRows = cache.getIfPresent(keyRow);
			if (cachedRows != null) {
				for (Row cachedRow : cachedRows) {
					collect(cachedRow);
				}
				return;
			}
		}

		for (int retry = 1; retry <= maxRetryTimes; retry++) {
			try {
				statement.clearParameters();
				for (int i = 0; i < keys.length; i++) {
					JDBCUtils.setField(statement, keySqlTypes[i], keys[i], i);
				}
				try (ResultSet resultSet = statement.executeQuery()) {
					if (cache == null) {
						if (resultSet.next()) {
							do {
								Row row = convertToRowFromResultSet(resultSet);
								LOG.debug("lookup result {}", row);
								collect(row);
							} while (resultSet.next());
						} else {
							Row row = new Row(outputSqlTypes.length);
							collect(row);
						}
					} else {
						if (resultSet.next()) {
							ArrayList<Row> rows = new ArrayList<>();
							do {
								Row row = convertToRowFromResultSet(resultSet);
								LOG.debug("lookup result {}", row);
								rows.add(row);
								collect(row);
							} while (resultSet.next());
							rows.trimToSize();
							cache.put(keyRow, rows);
						} else {
							Row row = new Row(outputSqlTypes.length);
							collect(row);
						}
					}
				}
				break;
			} catch (SQLException e) {
				LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
				if (retry >= maxRetryTimes) {
					throw new RuntimeException("Execution of JDBC statement failed.", e);
				}

				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
			}
		}
	}

	private Row convertToRowFromResultSet(ResultSet resultSet) throws SQLException {
		Row row = new Row(outputSqlTypes.length);
		for (int i = 0; i < outputSqlTypes.length; i++) {
			row.setField(i, getFieldFromResultSet(i, outputSqlTypes[i], resultSet));
		}
		return row;
	}

	private void establishConnection() throws SQLException, ClassNotFoundException {
		Class.forName(drivername);
		if (username == null) {
			dbConn = DriverManager.getConnection(dbURL);
		} else {
			dbConn = DriverManager.getConnection(dbURL, username, password);
		}
	}

	@Override
	public void close() throws IOException {
		if (cache != null) {
			cache.cleanUp();
			cache = null;
		}
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				LOG.info("JDBC statement could not be closed: " + e.getMessage());
			} finally {
				statement = null;
			}
		}

		if (dbConn != null) {
			try {
				dbConn.close();
			} catch (SQLException se) {
				LOG.info("JDBC connection could not be closed: " + se.getMessage());
			} finally {
				dbConn = null;
			}
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return keyTypes;
	}
}
