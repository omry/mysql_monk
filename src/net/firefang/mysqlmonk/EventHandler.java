package net.firefang.mysqlmonk;

import net.firefang.swush.Swush;

/**
 * @author omry 
 * @date Aug 11, 2009
 */
public interface EventHandler
{
	public void lagStarted(ServerDef server, String message);
	public void lagEnded(ServerDef server, String message);
	public void error(ServerDef server, String message, Exception ex);
	public void clearError(ServerDef server, String message);
	public void init(Swush sw) throws Exception;
	public void setMysqlMonk(MysqlMonk mySqlMonk);
}

