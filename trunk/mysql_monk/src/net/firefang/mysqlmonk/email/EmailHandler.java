package net.firefang.mysqlmonk.email;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import net.firefang.mysqlmonk.EventHandler;
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
	public void init(Swush sw) throws Exception
	{
		String host = sw.selectProperty("handler.smtp_host", "localhost");
		boolean debug = sw.selectBoolean("handler.smtp_debug", false);
		m_from = sw.selectProperty("handler.from", "mysqlmonk@localhost");
		m_recepients = implode(sw.selectFirst("handler.recepients").asArray(), ",");
		if (m_recepients.length() == 0) throw new Exception("Email receipients not defined");
		m_emailSender = new EmailSender(host, debug);
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
	public void lagEnded(ServerDef server, String message)
	{
		m_emailSender.send(m_from, m_recepients, "MySQLMonk -LAG: " + message, message);
	}

	@Override
	public void lagStarted(ServerDef server, String message)
	{
		m_emailSender.send(m_from, m_recepients, "MySQLMonk +LAG: " + message, message);
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

