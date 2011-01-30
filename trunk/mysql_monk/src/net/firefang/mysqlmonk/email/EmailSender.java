package net.firefang.mysqlmonk.email;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.log4j.Logger;

public class EmailSender
{
	static Logger logger = Logger.getLogger(EmailSender.class);
	
	private Session m_session;

	private ExecutorService m_threadPool;

	String host;
	boolean useSSL;
	boolean authUser;
	int port;
	String user;
	String password;
	
	public EmailSender(String mailhost, boolean useSSL, boolean auth, int port, String user, String password,boolean debug)
	{
		logger.info("Initializing EmailSender, mail host : " + mailhost + (debug  ? ", debug mode" : ""));
		
		this.host = mailhost;
		this.useSSL = useSSL;
		this.authUser = auth;
	    this.port = port;
	    this.user = user;
	    this.password = password;
		
		Properties props = System.getProperties();
        if (mailhost != null)
            props.put("mail.smtp.host", mailhost);
        if (useSSL){
        	props.put("mail.smtp.socketFactory.port", String.valueOf(this.port));
        	props.put("mail.smtp.socketFactory.class","javax.net.ssl.SSLSocketFactory");
        }
        if (auth)
        	props.put("mail.smtp.auth", "true");
 
        props.put("mail.smtp.port", String.valueOf(this.port));
        

        final String userName = user;
        final String userPassword = password;
        
        if (this.useSSL && this.authUser){
        	m_session = Session.getDefaultInstance(props,
        			new javax.mail.Authenticator()
        	{
        		protected PasswordAuthentication getPasswordAuthentication()
        		{ return new PasswordAuthentication(userName,userPassword);	}
        	});
        }else{
        	m_session = Session.getInstance(props, null);
        }
        
        m_session.setDebug(debug);
        
        
        m_threadPool = Executors.newCachedThreadPool(new ThreadFactory()
		{
        	AtomicInteger ii = new AtomicInteger();
			@Override
			public Thread newThread(Runnable r)
			{
				Thread t = new Thread(r);
				t.setName("Email sender thread " + (ii.getAndIncrement()));
				t.setDaemon(true);
				return t;
			}
		});
	}
	
	
	public void send(final String from, final String recepients, final String subject, final String message)
	{
		logger.info("Queuing email for delivery. recepients: " + recepients + ", subject : " + subject);
		m_threadPool.execute(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					Message msg = new MimeMessage(m_session);
					msg.setFrom(new InternetAddress(from));
					InternetAddress[] to = InternetAddress.parse(recepients, false);
					msg.setRecipients(Message.RecipientType.TO, to);
					msg.setSubject(subject);
					msg.setText(message);
					msg.setSentDate(new Date());
										
					Transport.send(msg);
				    
				} 
				catch (Exception e)
				{
					logger.error("Error sending email", e);
				}
			}
		});
	}
}
