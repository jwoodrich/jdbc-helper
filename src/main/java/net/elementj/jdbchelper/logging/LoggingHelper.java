package net.elementj.jdbchelper.logging;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Creates statements that log as needed.
 * @param <L> The object type that represents the logging level/priority for a given implementation.
 */
public interface LoggingHelper<L> {
	PreparedStatement prepareStatement(L level, String sql) throws SQLException;
	CallableStatement prepareCall(L level, String sql) throws SQLException;
/*	Future Functionality
    PreparedStatement wrap(PreparedStatement stm, L level, String sql) throws SQLException;
	CallableStatement wrap(CallableStatement stm, L level, String sql) throws SQLException;
*/
}
