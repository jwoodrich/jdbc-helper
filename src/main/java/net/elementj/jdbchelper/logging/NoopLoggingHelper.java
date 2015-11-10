package net.elementj.jdbchelper.logging;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * No-operation logging helper.  This is used to reduce the number of calls involved in using the jdbc-helper wrappers
 * when logging isn't enabled.
 */
public class NoopLoggingHelper implements LoggingHelper<LogLevel> {
	private final Connection connection;

	public NoopLoggingHelper(Connection connection) {
		super();
		this.connection = connection;
	}

	@Override
	public PreparedStatement prepareStatement(LogLevel level, String sql) throws SQLException {
		return connection.prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(LogLevel level, String sql) throws SQLException {
		return connection.prepareCall(sql);
	}
	
}
