import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.BasicConfigurator;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;


public class HostHandler 
{
	public HostHandler(String host)
	{	
		m_host = host;
		
		/* Récupération de la clé dsa pour initialiser la connexion ssh */
		FileHandler dsaKeyFile = new FileHandler(new String("/cal/homes/dutertre/.ssh/id_dsa"));
		String dsaKey = null;
		
		try { dsaKey = dsaKeyFile.getAllFileContent(); } 
		catch (IOException e1) { e1.printStackTrace(); }

		try { m_shell = new SSH(m_host, 22, "dutertre", dsaKey); } 
		catch (UnknownHostException e) { System.out.println("ERROR CONSTRUCTOR avec host : " + host); }
	}
	
	public String exec(String command) throws IOException
	{
		return new Shell.Plain(m_shell).exec(command);
	}
	
	public boolean isAlive()
	{
		try
		{
			this.exec("echo 'Test'");
			return true;
		}
		
		catch(Exception e)
		{
			return false;
		}
	}
	
	public String getName()
	{
		return m_host;
	}
	
	private String m_host;
	private Shell m_shell;
}
