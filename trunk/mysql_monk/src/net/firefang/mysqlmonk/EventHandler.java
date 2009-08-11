package net.firefang.mysqlmonk;

import net.firefang.swush.Swush;

/**
 * @author omry 
 * @date Aug 11, 2009
 */
public interface EventHandler
{
	public void lagStarted(Server server, String message);
	public void lagEnded(Server server, String message);
	public void error(Server server, String message, Exception ex);
	public void clearError(Server server, String message);
	public void init(Swush sw) throws Exception;
}

