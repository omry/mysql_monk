package net.firefang.mysqlmonk.email;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.firefang.mysqlmonk.EventHandler;
import net.firefang.mysqlmonk.MysqlMonk;
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
			List<MysqlProc> proclist = getProcList(c);
			StringBuffer sb = new StringBuffer();
			sb.append(message);
			sb.append("\n");
			sb.append("Mysql process list when this messages was triggered : \n");
			sb.append(Utils.rpad("id", 8, ' '));
			sb.append(Utils.rpad("user", 14, ' '));
			sb.append(Utils.rpad("host", 30, ' '));
			sb.append(Utils.rpad("db", 16, ' '));
			sb.append(Utils.rpad("command", 20, ' '));
			sb.append(Utils.rpad("time", 6, ' '));
			sb.append(Utils.rpad("state", 16, ' '));
			sb.append(Utils.rpad("info", 8, ' '));
			sb.append("\n");
			Set<String> commands = new HashSet<String>();
			Map<String, Map<String, Integer>> host2SleepCount = new HashMap<String, Map<String,Integer>>(); 
			for(MysqlProc p : proclist)
			{
				String hostName = p.getHostName();
				Map<String, Integer> mm = host2SleepCount.get(hostName);
				if (mm == null) host2SleepCount.put(hostName, mm = new HashMap<String, Integer>());
				Integer ii = mm.get(p.command);
				if (ii == null) ii = 0;
				mm.put(p.command, ++ii);
				commands.add(p.command);
				
				if (p.command.equals("Sleep")) continue;
				
				sb.append(Utils.rpad(""+p.id, 8, ' '));
				sb.append(Utils.rpad(p.user	, 14, ' '));
				sb.append(Utils.rpad(p.host	, 30, ' '));
				sb.append(Utils.rpad(p.db	, 16, ' '));
				sb.append(Utils.rpad(p.command, 20, ' '));
				sb.append(Utils.rpad(""+p.time	, 6, ' '));
				sb.append(Utils.rpad(p.state	, 16, ' '));
				sb.append(p.info);
				sb.append("\n");
			}
			
			sb.append("\n");
			if (host2SleepCount.size() > 0)
			{
				sb.append(Utils.rpad("hostname", 30, ' '));
				for(String command : Utils.getSorted(commands))
				{
					sb.append(Utils.rpad(command, 16, ' '));
				}
				sb.append("\n");
				for(String hostname : Utils.getSortedKeys(host2SleepCount))
				{
					sb.append(Utils.rpad(hostname, 30, ' '));
					Map<String, Integer> mm = host2SleepCount.get(hostname);
					for(String command : Utils.getSorted(commands))
					{
						Integer ii = mm.get(command);
						sb.append(Utils.rpad("" + (ii != null ? ii : " "), 16, ' '));
					}
					sb.append("\n");
				}
			}
			
			message = sb.toString();
		}
		m_emailSender.send(m_from, m_recepients, title, message);
	}
	
	static class MysqlProc
	{
		int id;
		String user;
		String host;
		String db;
		String command;
		int time;
		String state;
		String info;
		
		@Override
		public String toString()
		{
			return id + " " + user + " " + host + " " + db + " " + command + " " + time + " " + state + " " + info; 
		}

		public String getHostName()
		{
			int i = host.indexOf(':');
			if (i == -1)
				return host;
			else 
				return host.substring(0, i);
		}
	}
	
	private List<MysqlProc> getProcList(Connection c) throws SQLException
	{
		List<MysqlProc> res = new ArrayList<EmailHandler.MysqlProc>();
		Statement st = c.createStatement();
		ResultSet rs = null;
		try
		{
			st.execute("SHOW PROCESSLIST");
			rs = st.getResultSet();
			while(rs.next())
			{
				MysqlProc p = new MysqlProc();
				p.id = rs.getInt("id");
				p.user = rs.getString("user");
				p.host = rs.getString("host");
				p.db = rs.getString("db");
				p.command = rs.getString("command");
				p.time = rs.getInt("time");
				p.state = rs.getString("state");
				p.info = rs.getString("info");
				res.add(p);
			}
		}
		finally
		{
			if (rs != null)
				rs.close();
			st.close();
		}
		return res;
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

