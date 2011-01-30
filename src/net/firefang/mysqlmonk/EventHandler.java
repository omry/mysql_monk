package net.firefang.mysqlmonk;

import java.sql.Connection;

import net.firefang.swush.Swush;

/**
 * @author omry 
 * @date Aug 11, 2009
 */
public interface EventHandler
{
	public void lagStarted(ServerDef server, String message, Connection c) throws Exception;
	public void lagEnded(ServerDef server, String message, Connection c) throws Exception;
	public void error(ServerDef server, String message, Exception ex) throws Exception;
	public void clearError(ServerDef server, String message) throws Exception;
	public void init(Swush sw) throws Exception;
	public void setMysqlMonk(MysqlMonk mySqlMonk);
}

