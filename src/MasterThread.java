public class MasterThread extends Thread 
{
	private String m_hostname;
	private String m_command;
	private boolean m_hostUsable;
	 
	public MasterThread(String hostname, String command) 
	{
	    this.m_hostname = hostname;
	    this.m_command = command;
	    this.m_hostUsable = true;
	}
	
	@Override
	public void run()
	{	
		HostHandler tmpHost = new HostHandler(m_hostname);
		
		if( tmpHost.isAlive() )
		{
			try{tmpHost.exec(m_command);}
			catch (Exception e) {m_hostUsable = false;}
		}
			
		else
		{
			//System.out.println("Thread " + this.m_hostname + " : HOST UNREACHABLE...");
			m_hostUsable = false;
		}
	}
	
	public boolean isHostUsable()
	{
		return m_hostUsable;
	}
	
	public String getHost()
	{
		return m_hostname;
	}
}
