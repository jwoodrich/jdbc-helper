package net.elementj.jdbchelper;

import java.sql.Connection;
import java.sql.ResultSet;

import net.elementj.jdbchelper.logging.LogLevel;
import net.elementj.jdbchelper.logging.LoggingHelper;
import net.elementj.jdbchelper.logging.NoopLoggingHelper;
import net.elementj.jdbchelper.logging.SLF4JLoggingHelper;

/**
 * JDBC helper object factory.
 */
public final class JDBCHelper {
	private JDBCHelper() { }
	/**
	 * Create an Iterator-like wrapper around a ResultSet. 
	 * @param rs The ResultSet
	 * @return The wrapper
	 */
	public static ResultSetIterator iterator(ResultSet rs) {
		return new ResultSetIterator(rs);
	}
	/**
	 * Create an Iterator-like wrapper around a ResultSet, starting at a specific index.
	 * @param rs The ResultSet
	 * @param startIndex The start index
	 * @return The wrapper
	 */
	public static ResultSetIterator iterator(ResultSet rs, int startIndex) {
		return new ResultSetIterator(rs, startIndex);
	}
	/**
	 * Returns an object factory for logging functionality with SLF4J.
	 * @param connection The JDBC connection.
	 * @param logger The SLF4J logger.
	 * @return The object factory 
	 */
	public static LoggingHelper<LogLevel> logging(Connection connection, org.slf4j.Logger logger) {
		return new SLF4JLoggingHelper(connection,logger);
	}
	/**
	 * Returns an object factory for logging functionality with SLF4J based on the value of a boolean.
	 * Usage: JDBCHelper.logging(connection,logger,logger.isDebugEnabled())... etc
	 * @param connection The JDBC connection.
	 * @param logger The SLF4J logger.
	 * @param enabled An indicator if logging is enabled or not.
	 * @return The appropriate object factory based on the logging status
	 */
	public static LoggingHelper<LogLevel> logging(Connection connection, org.slf4j.Logger logger, boolean enabled) {
		return enabled?new SLF4JLoggingHelper(connection,logger):new NoopLoggingHelper(connection);
	}
}
