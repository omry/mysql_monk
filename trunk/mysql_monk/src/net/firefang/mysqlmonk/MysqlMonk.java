package net.firefang.mysqlmonk;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import net.firefang.swush.Swush;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.xml.DOMConfigurator;
import org.logicalcobwebs.proxool.ProxoolException;
import org.logicalcobwebs.proxool.ProxoolFacade;

/**
 * @author omry 
 * @date Aug 10, 2009
 */
public class MysqlMonk 
{
	public static Logger logger = Logger.getLogger(MysqlMonk.class);
	
	int checkInterval;
	int updateInterval;
	
	private Set<String> s_hosts;
	private Map<String, ServerDef> m_serversMap;
	private Map<String, ServerDef> m_aliasesMap;
	private List<EventHandler> eventHandlers;

	private Thread m_masterUpdater;
	
	private boolean m_running = true;

	public static void main(String[] args) throws Exception
	{
		new MysqlMonk(args);
	}
	
	public MysqlMonk(String args[]) throws Exception
	{
		
		CmdLineParser p = new CmdLineParser();
		p.addStringOption('c', "conf");
		p.addBooleanOption("install");
		p.addBooleanOption("list");
		p.addStringOption("server");
		p.parse(args);
		
		
		String conf = (String) p.getOptionValue("conf", "conf/monk.conf");
		File dir = new File(conf).getAbsoluteFile().getParentFile();
		DOMConfigurator.configure(dir + "/log4j.xml");
		loadConfiguration(conf);
		
		boolean install = (Boolean)p.getOptionValue("install", Boolean.FALSE);
		boolean list = (Boolean)p.getOptionValue("list", Boolean.FALSE);
		if (install)
		{
			String sid = (String)p.getOptionValue("server", -1);
			if (sid == null) throw new RuntimeException("server not specified");
			
			ensureInstalled(sid);
			
			return;
		}
		else
		if (list)
		{
			List<String> servers = new ArrayList<String>(m_serversMap.keySet());
			Collections.sort(servers);
			for(String s : servers)
			{
				ServerDef server = m_serversMap.get(s);
				boolean hasAlias = m_aliasesMap.containsKey(server.host);
				logger.info(s  + (hasAlias ? " (alias = " + server.host + ")" : ""));
			}
		}
		else
		{
			monitor();
		}

	}
	
	

	public List<EventHandler> getEventHandlers() {
		return eventHandlers;
	}

	
	private boolean isInstalled(String sid) throws SQLException
	{
		ServerDef server = m_serversMap.get(sid);
		Connection c = DriverManager.getConnection(server.getConnectionAlias());
		try
		{
			return isInstalled(c, server.dbName);
		}
		finally
		{
			c.close();
		}				
	}
	
	private boolean isInstalled(Connection c, String dbName) throws SQLException
	{
		String sql = "SHOW TABLES IN "+dbName+" LIKE 'mysql_monk'";
		String b = getVar(c, sql);
		return b != null;
	}
	
	private void ensureInstalled(String sid) throws SQLException
	{
		ServerDef server = getServer(sid);
		Connection c = DriverManager.getConnection(server.getConnectionAlias());
		try
		{
			if (!isInstalled(sid))
			{
				logger.debug("Ensuring mysql_monk table is installed in " + server.niceName());
				String create = "CREATE TABLE IF NOT EXISTS " + server.dbName + ".mysql_monk ("
				+ "master_id INT NOT NULL ,"
				+ "last_update DATETIME NOT NULL ,"
				+ "PRIMARY KEY ( master_id )"
				+ ") ENGINE = InnoDB";
				query(c, create);
			}
		}
		finally
		{
			c.close();
		}
	}

	private void monitor() throws Exception
	{
		startUpdateThread();
		startChecking();
	}

	private void startChecking()
	{
		int ns = 0;
		for (final ServerDef s : m_serversMap.values())	if (s.isSlave) ns++;
		
		ExecutorService pool = getThreadPool(Math.max(1, ns), "SlaveChecker_");
		logger.info("Starting checker thread, checking every " + checkInterval + " seconds");
		while(m_running)
		{
			final CountDownLatch latch = new CountDownLatch(ns);
			for (final ServerDef s : m_serversMap.values())
			{
				if (s.isSlave)
				{
					pool.execute(new Runnable()
					{
						public void run() 
						{
							synchronized (s)
							{
								long now = System.currentTimeMillis() / 1000;
								try
								{
									Connection c = DriverManager.getConnection(s.getConnectionAlias());
									if (!s.m_installed)
									{
										ensureInstalled(s.getID());
										s.m_installed = true;
									}
									
									try
									{
										ServerDef master = getServer(s.master);
										logger.debug("Checking lag in " + s.niceName());
										String lastUpdate = getVar(c, "SELECT UNIX_TIMESTAMP(last_update) FROM " + s.dbName + ".mysql_monk WHERE master_id = " + master.mysqlServerID);
										if (lastUpdate != null)
										{
											long slaveUpdateTime = Long.parseLong(lastUpdate);
											long masterUpdateTime = master.updateTime;
											if (masterUpdateTime != -1) // master was actually updated
											{
												long oldLag = s.slaveLag; 
												s.slaveLag =  masterUpdateTime - slaveUpdateTime;
												s.maxLagSeen = Math.max(s.maxLagSeen, s.slaveLag);
												
												if (oldLag != -1)
												{
													if (oldLag < s.m_maxAllowedLag && s.slaveLag >= s.m_maxAllowedLag)
													{
														String msg = s.niceName() + " is lagging behind master " + master.niceName() + " by "+Utils.formatTimeLengthSec(s.slaveLag)+" which is more than the allowed " + Utils.formatTimeLengthSec(s.m_maxAllowedLag)  + " lag for this server";
														_lagStarted(s, msg,c );
													}
													
													if (oldLag >= s.m_maxAllowedLag && s.slaveLag < s.m_maxAllowedLag)
													{
														_lagEnded(s, "Server " + s.niceName() + " is no longer lagging behind master " + master.niceName() + ", worse lag seen was " + s.maxLagSeen + " seconds",c);
														s.maxLagSeen = 0;
													}
												}
											}
										}
									}
									finally
									{
										c.close();
									}
									s.updateTime = now;
									
									if (s.inError)
									{
										s.inError = false;
										_clearError(s, "Error cleared");
									}
								}
								catch (SQLException e)
								{
									String error = "SQLException";
									if (e instanceof com.mysql.jdbc.CommunicationsException)
									{
										error = "Can't connect to database";
									}
									if (!s.inError)
									{
										_error(s, error, e);
										s.inError = true;
									}
								}
								finally
								{
									latch.countDown();
								}
							}
						}
					});
				}
			}
			
			try
			{
				latch.await();
			}
			catch (InterruptedException e1)
			{
			}
			
			
			wait1(checkInterval);
		}
	}

	
	private void startUpdateThread()
	{
		m_masterUpdater = new Thread("Master update thread")
		{
			@Override
			public void run()
			{
				logger.info("Updating masters every " + updateInterval + " seconds");
				
				final CountDownLatch latch = new CountDownLatch(m_serversMap.size());
				int nm = 0;
				for (ServerDef s : m_serversMap.values()) if (s.isMaster) nm++;
				ExecutorService pool = getThreadPool(nm, "MasterUpdater_");
				
				while(m_running)
				{
					final long now = System.currentTimeMillis() / 1000;
					for (final ServerDef s : m_serversMap.values())
					{
						if (s.isMaster)
						{
							pool.execute(new Runnable()
							{
								@Override
								public void run()
								{
									synchronized (s)
									{
										try
										{
											if (!s.m_installed)
											{
												ensureInstalled(s.getID());
												s.m_installed = true;
											}
											
											Connection c = DriverManager.getConnection(s.getConnectionAlias());
											try
											{
												String mysqlServerID = getServerID(c);
												if (mysqlServerID == null)
												{
													if (!s.inError)
													{
														_error(s, "server_id not configured", null);
														s.inError = true;
													}
												}
												else
												{
													query(c, "REPLACE INTO " + s.dbName + ".mysql_monk(master_id,last_update) VALUES("+mysqlServerID+", FROM_UNIXTIME("+now+"))");
													s.mysqlServerID = Integer.parseInt(mysqlServerID);
												}
											}
											finally
											{
												c.close();
											}
											s.updateTime = now;
											if (s.inError)
											{
												s.inError = false;
												_clearError(s, "Error cleared");
											}								
										}
										catch (SQLException e)
										{
											if (!s.inError)
											{
												_error(s, "SQLException", e);
												s.inError = true;
											}
											
										}
										finally
										{
											latch.countDown();
										}
									}
								}
							});
						}
					}
					
					try
					{
						latch.await();
					}
					catch (InterruptedException e1)
					{
					}				
					
					
					wait1(updateInterval);
				}
			}
		};
		
		m_masterUpdater.start();
	}

	static String getServerID(Connection c) throws SQLException
	{
		return getVar(c, "SELECT @@server_id");
	}

	private static String getVar(Connection c, String sql) throws SQLException
	{
		Statement st = c.createStatement();
		try
		{
			ResultSet res = st.executeQuery(sql);
			if (res.next())
			{
				return res.getString(1);
			}
		}
		finally
		{
			st.close();
		}
		return null;
	}

	private void loadConfiguration(String name) throws Exception
	{
		logger.info("Loading " + name);
		
		s_hosts = new HashSet<String>();
		
		Swush conf = new Swush(new File(name));
		
		updateInterval = conf.selectIntProperty("mysql_monk.monitor.update_interval", 10);
		int defaultMaxAllowedLag =  conf.selectIntProperty("mysql_monk.monitor.max_allowed_lag", 20);
		int defaultSocketTimeout =  conf.selectIntProperty("mysql_monk.monitor.socket_timeout_sec", 20);
		int defaultConnectionTimeout =  conf.selectIntProperty("mysql_monk.monitor.connection_timeout_sec", 20);
		checkInterval = conf.selectIntProperty("mysql_monk.monitor.check_interval", 5);
		
		List<Swush> servers = conf.select("mysql_monk.server");
		m_serversMap = new HashMap<String, ServerDef>();
		m_aliasesMap = new HashMap<String, ServerDef>();
		
		Set<String> removed = new HashSet<String>(); 
		
		for(Swush sw : servers)
		{
			ServerDef s = new ServerDef(sw, defaultMaxAllowedLag, defaultConnectionTimeout, defaultSocketTimeout);
			String id = s.getID();
			ServerDef old = m_serversMap.put(id, s);
			if (old != null) throw new IllegalArgumentException("Duplicate server with id " + id);
			
			ServerDef ss = m_aliasesMap.get(s.host);
			if (ss != null)
			{
				removed.add(s.host); // host run multiple servers, shoud use fully qualified id (host_port) to access.
			}
			else
			{
				m_aliasesMap.put(s.host, s);
			}
			
			s_hosts.add(s.host);
		}
		
		for(String rem : removed)
		{
			m_aliasesMap.remove(rem);
		}
		
		
		for(ServerDef s : m_serversMap.values())
		{
			String masterID = s.master;
			if (masterID != null)
			{
				if (isServer(masterID))
				{
					ServerDef master = getServer(masterID);
					master.isMaster = true;
					s.isSlave = true;
				}
				else
				{
					if (s_hosts.contains(masterID))
					{
						throw new RuntimeException(masterID + " is not unique because there is more than one server on this host, use fully qualified id (host_port)");
					}
				}
			}
			
			registerConnectionPool(s.getConnectionAlias(), s.host, s.port, s.dbName, s.user, s.password);
			logger.info("Added server : " + s);
		}
		
		eventHandlers = new ArrayList<EventHandler>();
		for(Swush sw : conf.select("mysql_monk.handlers.handler"))
		{
			String clazz = sw.selectProperty("handler.class");
			EventHandler handler = (EventHandler) Class.forName(clazz).newInstance();
			handler.init(this, sw);
			eventHandlers.add(handler);
			logger.debug("Adding event handler : " + handler.getClass().getName());
		}
		
		if (eventHandlers.size() == 0)
		{
			logger.warn("No event handlers specified!");
		}
	}

	public Set<String> getServerNames()
	{
		return m_serversMap.keySet();
	}
	
	public ServerDef getServer(String id)
	{
		ServerDef server = m_serversMap.get(id);
		if (server == null)
			server = m_aliasesMap.get(id);
		
		if (server == null) throw new IllegalArgumentException("Server not found, id " + id + ", use --list to view available servers");
		return server;
	}
	
	private boolean isServer(String id)
	{
		ServerDef server = m_serversMap.get(id);
		if (server == null)
			server = m_aliasesMap.get(id);
		
		if (server == null) 
			return false;
		
		return true;
	}
	
	
	public static void registerConnectionPool(String alias, String server, int port, String dbname, String username, String password) throws ProxoolException
	{
		try
		{
			Class.forName("org.logicalcobwebs.proxool.ProxoolDriver");
		}
		catch (ClassNotFoundException e)
		{
			throw new ProxoolException(e);
		}
		Properties info = new Properties();
		info.setProperty("proxool.maximum-connection-count", "10");
		info.setProperty("proxool.house-keeping-test-sql", "select VERSION()");
		info.setProperty("user", username);
		info.setProperty("password", password);
		info.setProperty("proxool.driver-class", "com.mysql.jdbc.Driver");
		String url = alias + ":" + "com.mysql.jdbc.Driver" + ":jdbc:mysql://" + server + ":"+port+"/" + dbname + "?characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull";
		try
		{
			ProxoolFacade.registerConnectionPool(url, info);
		}
		catch (Exception e)
		{
			logger.error(e);
		}
	}
	
	public static void query(Connection conn, String sql) throws SQLException
	{
		Statement st = null;
		try
		{
			st = conn.createStatement();
			st.execute(sql);
		}
		finally
		{
			if (st != null) st.close();
		}
	}
	
	
	public static void configureLog4J()
	{
	    Logger root = Logger.getRootLogger();
    	root.setLevel(Level.DEBUG);
    	ConsoleAppender consoleAppender = new ConsoleAppender(new PatternLayout("%d{HH:mm:ss.SSS} %c{2} [%t] %p - %m%n"));
    	consoleAppender.setName("CONSOLE");
    	if (root.getAppender("CONSOLE") == null)
    		root.addAppender(consoleAppender);
    	Logger.getLogger("org.logicalcobwebs").setLevel(Level.WARN);
	}
	
	void _lagStarted(ServerDef server, String message, Connection c)
	{
		for(EventHandler handler : eventHandlers)
		{
			try
			{
				handler.lagStarted(server, message, c);
			}
			catch (Exception e)
			{
				logger.error("_lagStarted : Error from handler " + handler.getClass().getName(), e);
			}
		}
	}
	
	void _lagEnded(ServerDef server, String message, Connection c)
	{
		for(EventHandler handler : eventHandlers)
		{
			try
			{
				handler.lagEnded(server, message, c);
			}
			catch (Exception e)
			{
				logger.error("_lagEnded : Error from handler " + handler.getClass().getName(), e);
			}
		}
	}
	
	void _error(ServerDef server, String message, Exception ex)
	{
		logger.info("Error in " + server.niceName()  + " : " + server, ex);
		for(EventHandler handler : eventHandlers)
		{
			try
			{
				handler.error(server, message, ex);
			}
			catch (Exception e)
			{
				logger.error("_error : Error from handler " + handler.getClass().getName(), e);
			}			
		}	
	}
	
	void _clearError(ServerDef server, String message)
	{
		logger.info("Error cleared in " + server.niceName()  + " : " + server);
		for(EventHandler handler : eventHandlers)
		{
			try
			{
				handler.clearError(server, message);
			}
			catch (Exception e)
			{
				logger.error("_clearError : Error from handler " + handler.getClass().getName(), e);
			}
		}	
	}

	public void stop() throws Exception
	{
		logger.info("Stopping MysqlMonk");
		m_running = false;
		m_masterUpdater.interrupt();
		
		try
		{
			m_masterUpdater.join();
		}
		catch (InterruptedException e)
		{
		}
		
		synchronized (this)
		{
			notifyAll();
		}
	}
	
	private ExecutorService getThreadPool(int nt, final String namePrefix)
	{
		return Executors.newFixedThreadPool(Math.max(1, nt), new ThreadFactory()
		{
			int id = 0;
			@Override
			public Thread newThread(final Runnable r)
			{
				return new Thread(namePrefix + (id++))
				{
					@Override
					public void run()
					{
						r.run();
					}
				};
			}
		});
	}

	private void wait1(int waitSec)
	{
		synchronized (this)
		{
			if (m_running)
			{
				try
				{
					wait(waitSec * 1000);
				}
				catch (InterruptedException e)
				{
				}
			}
		}
	}
	
}
