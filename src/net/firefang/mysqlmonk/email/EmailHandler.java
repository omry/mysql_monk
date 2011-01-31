package net.firefang.mysqlmonk.email;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.firefang.mysqlmonk.EventHandler;
import net.firefang.mysqlmonk.MysqlMonk;
import net.firefang.mysqlmonk.MysqlProc;
import net.firefang.mysqlmonk.ServerDef;
import net.firefang.mysqlmonk.Utils;
import net.firefang.swush.Swush;

/**
 * @author omry 
 * @date Aug 11, 2009
 */
public class EmailHandler implements EventHandler
{
	EmailSender m_emailSender;
	
	String m_from;
	String m_recepients;

	@Override
	public void init(MysqlMonk monk, Swush handlerConf) throws Exception
	{
		String host = handlerConf.selectProperty("handler.smtp_host", "localhost");
		boolean debug = handlerConf.selectBoolean("handler.smtp_debug", false);
		m_from = handlerConf.selectProperty("handler.from", "mysqlmonk@localhost");
		m_recepients = implode(handlerConf.selectFirst("handler.recepients").asArray(), ",");
		if (m_recepients.length() == 0) throw new Exception("Email receipients not defined");
		
		boolean useSSL = handlerConf.selectBoolean("handler.use_ssl", false);
		boolean auth = handlerConf.selectBoolean("handler.auth_user", false);
		int port = handlerConf.selectIntProperty("handler.port", 25);
		String user = handlerConf.selectProperty("handler.user","");
		String password = handlerConf.selectProperty("handler.password","");
		
		m_emailSender = new EmailSender(host,useSSL,auth,port,user,password,debug);
	}	
	
	@Override
	public void error(ServerDef server, String message, Exception ex)
	{
		String email = server.niceName() + ":\n\nAn error occured in MySQLMonk :\n" + message + "\n";
		if (ex != null)
		{
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(bout, true);
			ex.printStackTrace(ps);
			String st = new String(bout.toByteArray());
			email += st;
		}
		m_emailSender.send(m_from, m_recepients, "MySQLMonk +ERROR : " + server.niceName() + " : " + message, email);
	}

	
	@Override
	public void clearError(ServerDef server, String message)
	{
		m_emailSender.send(m_from, m_recepients, "MySQLMonk -ERROR : " + server.niceName() + " : " + message, message);
	}		
	
	@Override
	public void lagEnded(ServerDef server, String message, Connection c)
	{
		m_emailSender.send(m_from, m_recepients, "MySQLMonk -LAG: " + message, message);
	}

	@Override
	public void lagStarted(ServerDef server, String message, Connection c) throws SQLException
	{
		String title = "MySQLMonk +LAG: " + message;
		
		if (c != null)
		{
			
			
			StringBuffer sb1 = new StringBuffer();
			sb1.append(message);
			
			List<MysqlProc> proclist = Utils.getProcList(c);
			
			sb1.append(Utils.getProcTable(proclist));
			
			message = sb1.toString();
		}
		m_emailSender.send(m_from, m_recepients, title, message);
	}

	public static void crunchProcessList(List<MysqlProc> proclist, Set<String> commands, Map<String, Map<String, Integer>> host2CommandCount)
	{
		for(MysqlProc p : proclist)
		{
			String hostName = p.getHostName();
			Map<String, Integer> mm = host2CommandCount.get(hostName);
			if (mm == null) host2CommandCount.put(hostName, mm = new HashMap<String, Integer>());
			Integer ii = mm.get(p.command);
			if (ii == null) ii = 0;
			mm.put(p.command, ++ii);
			commands.add(p.command);
		}
	}
	
	private static String implode(Object set[], String sep)
	{
		StringBuffer b = new StringBuffer(set.length * 10);

		if (set.length > 0) b.append(set[0]);
		for (int i = 1; i < set.length; i++)
		{
			b.append(sep).append(set[i]);
		}
		return b.toString();
	}
}

