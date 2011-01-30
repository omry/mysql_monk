package net.firefang.mysqlmonk.email;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import net.firefang.mysqlmonk.EventHandler;
import net.firefang.mysqlmonk.MysqlMonk;
import net.firefang.mysqlmonk.ServerDef;
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
			sb.append("id\tuser\thost\tdb\tcommand\ttime\tstate\tinfo\n");
			for(MysqlProc p : proclist)
			{
				sb.append(p.id).append("\t");
				sb.append(p.user).append("\t");
				sb.append(p.host).append("\t");
				sb.append(p.db).append("\t");
				sb.append(p.command).append("\t");
				sb.append(p.state).append("\t");
				sb.append(p.info);
				sb.append("\n");
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

