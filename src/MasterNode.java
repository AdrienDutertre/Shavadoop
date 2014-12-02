import java.util.HashMap;
import java.util.List;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class MasterNode 
{
	private String m_inputFilename;
	private String m_listOfPotentialHostsFilename;
	private int m_nbSlaveNodes;
	private ArrayList<String> m_listOfUsableHosts;
	private HashMap<Integer, String> m_dictionaryUM_HostName; 
	private HashMap<String, ArrayList<Integer>> m_keysDictionary;
	
	public MasterNode(String inputFilename, String listOfPotentialHosts, int nbSlavesToUse) throws IOException, InterruptedException
	{
		m_inputFilename = inputFilename;
		m_listOfPotentialHostsFilename = listOfPotentialHosts;
		m_nbSlaveNodes = nbSlavesToUse;
		
		m_listOfUsableHosts = new ArrayList<String>();
		m_dictionaryUM_HostName = new HashMap<Integer, String>();
	    m_keysDictionary = new HashMap<String, ArrayList<Integer>>();
		getUsableHosts();
	}
	
	private List<String> getPotentialHosts() throws IOException
	{
		FileHandler hostListFile = new FileHandler(new String(m_listOfPotentialHostsFilename));
		String currentLine = null;
		ArrayList<String> hostList = new ArrayList<String>();
		while( ( currentLine = hostListFile.getNextLine() ) != null )
			hostList.add(currentLine);
		
		return hostList;
	}
	
	public void getUsableHosts() throws IOException, InterruptedException
	{
		List<String> potentialHosts = getPotentialHosts();
		
		/* Récupération d'une liste de threads */
		ArrayList<MasterThread> listOfThreads = new ArrayList<MasterThread>();
		for(String host : potentialHosts)
		{
			MasterThread tmpThread = new MasterThread(host, "echo Test");
			listOfThreads.add(tmpThread);
		}
		
		/* A partir de cette liste de threads on teste si c'est alive ou non */
		for(MasterThread th : listOfThreads)
			th.start();
		
		for(MasterThread th : listOfThreads)
			th.join();
		
		for(MasterThread th : listOfThreads)
		{
			if( th.isHostUsable() )
				m_listOfUsableHosts.add(th.getHost());
		}
	}
	
	public void splitInputFile() throws IOException
	{
		System.out.println("\nDébut du split");
		int startTime = (int) System.currentTimeMillis();
		
		/* Ouverture du fichier input et récupération de ses lignes */
		File inputFile = new File(m_inputFilename);
		List<String> lines = FileUtils.readLines(inputFile, new String("UTF-8"));
		
		/* Création de tous les fichiers Si */
		ArrayList<File> splitFileList = new ArrayList<File>();
		for(int i=0; i<m_nbSlaveNodes; i++)
		{
			FileUtils.deleteQuietly( new File("/cal/homes/dutertre/INF_727/Shavadoop/S" + new Integer(i).toString() ) );
			File fileTmp = new File("/cal/homes/dutertre/INF_727/Shavadoop/S" + new Integer(i).toString() );
			splitFileList.add( fileTmp );
		}
			
		/* Ecriture de chaque fichier Si */
		for(int j = 0; j<m_nbSlaveNodes; j++)
		{
			String currentText = StringUtils.join( lines.subList(j*lines.size()/m_nbSlaveNodes, (j+1)*lines.size()/m_nbSlaveNodes), "\n");
			FileUtils.write(splitFileList.get(j), currentText, true);
		}
		
		int endTime = (int) System.currentTimeMillis();
		System.out.println("Fin du split => Exécution en " + new Integer(endTime-startTime).toString() + "ms");
	}
	
	public void performMapping() throws InterruptedException
	{ 
		System.out.println("\nDébut du mapping");
		int startTime = (int) System.currentTimeMillis();
		
		/* On se crée une liste de MasterThreads */
		ArrayList<MasterThread> listOfThreads = new ArrayList<MasterThread>();
		
		for(int i=0; i<m_nbSlaveNodes; i++)
		{
			m_dictionaryUM_HostName.put(new Integer(i), m_listOfUsableHosts.get(i));
			String command = "java -jar /cal/homes/dutertre/INF_727/Shavadoop/Mapper.jar " + new Integer(i).toString() + " " + new Integer(m_nbSlaveNodes).toString();
			MasterThread tmpThread = new MasterThread(m_listOfUsableHosts.get(i), command);
			listOfThreads.add(tmpThread);
		}
		
		/* Execution */
		for(MasterThread th : listOfThreads)
			th.start();
		
		for(MasterThread th : listOfThreads)
			th.join();
		
		int endTime = (int) System.currentTimeMillis();
		System.out.println("Fin du mapping => Exécution en " + new Integer(endTime-startTime).toString() + "ms");
		
	}
	
	public void performShuffle() throws IOException, InterruptedException
	{
		System.out.println("\nDébut du shuffle");
		int startTime = (int) System.currentTimeMillis();
		
		/* Boucle sur le nombre de threads pour obtenir leurs clés */
		for(int threadNumber = 0; threadNumber<m_nbSlaveNodes; threadNumber++)
		{
			/* Ouverture et récupération des lignes */
			File keysFile = new File("/cal/homes/dutertre/INF_727/Shavadoop/Keys" + new Integer(threadNumber).toString() );
			List<String> lines = FileUtils.readLines(keysFile, new String("UTF-8"));
			
			/* Ajout de cette clé au dictionnaire */
			for(String key : lines)
			{
				/* Cas où c'est la première fois que l'on rencontre cette clé */
				if( m_keysDictionary.containsKey(key) == false )
				{
					ArrayList<Integer> tmpList = new ArrayList<Integer>();
					tmpList.add( new Integer(threadNumber) );
					m_keysDictionary.put(key, tmpList);
				}
				
				/* Cas où la clé était déjà présente */
				else
					m_keysDictionary.get(key).add( new Integer(threadNumber) );
			}
		}
		
//		/* Création de tous les fichiers SHi */
//		ArrayList<File> shuffleFileList = new ArrayList<File>();
//		for(int i=0; i<m_nbSlaveNodes; i++)
//		{
//			FileUtils.deleteQuietly( new File("/cal/homes/dutertre/INF_727/Shavadoop/SH" + new Integer(i).toString() ) );
//			File fileTmp = new File("/cal/homes/dutertre/INF_727/Shavadoop/SH" + new Integer(i).toString() );
//			shuffleFileList.add( fileTmp );
//		}
//		
//		ArrayList<String> allKeysAndUms = new ArrayList<String>();
//		int numberOfKeysProcessed = 0;
//		for(String key : m_keysDictionary.keySet())
//		{
//			/* Création du string renseignant les ums ayant la clé courante */
//			String umNumbers = new String();
//			for(Integer i : m_keysDictionary.get(key))
//				umNumbers += i.toString() + ";";
//			
//			allKeysAndUms.add(key + " " + umNumbers);
//		}
//		
//		/* Ecriture de chaque fichier SHi */
//		for(int j = 0; j<m_nbSlaveNodes; j++)
//		{
//			String currentText = StringUtils.join( allKeysAndUms.subList(j*allKeysAndUms.size()/m_nbSlaveNodes, (j+1)*allKeysAndUms.size()/m_nbSlaveNodes), "\n");
//			FileUtils.write(shuffleFileList.get(j), currentText, true);
//		}
		
		/* Paramétrage du thread avec la bonne commande */
		ArrayList<MasterThread> listOfThreads = new ArrayList<MasterThread>();
		for(int i = 0; i<m_nbSlaveNodes; i++)
		{
			String command = "java -jar /cal/homes/dutertre/INF_727/Shavadoop/Shuffler.jar " + i + " " + m_nbSlaveNodes;
			MasterThread tmpThread = new MasterThread(m_listOfUsableHosts.get(i), command);
			listOfThreads.add(tmpThread);
		}
		
		/* Execution de la liste des threads */
		for(MasterThread th : listOfThreads)
			th.start();
		
		for(MasterThread th : listOfThreads)
			th.join();
		
		int endTime = (int) System.currentTimeMillis();
		System.out.println("Fin du shuffle => Exécution en " + new Integer(endTime-startTime).toString() + "ms");
	}
	
	
	public void performReduce() throws InterruptedException
	{
		System.out.println("\nDébut du reduce");
		int startTime = (int) System.currentTimeMillis();
		
		/* On se contente de lancer les threads en leur donnant en argument leur index pour qu'ils sachent quel fichier SMi lire et traiter */
		ArrayList<MasterThread> listOfThreads = new ArrayList<MasterThread>();
		for(int i = 0; i<m_nbSlaveNodes; i++)
		{
			String command = "java -jar /cal/homes/dutertre/INF_727/Shavadoop/Reducer.jar " + new Integer(i).toString();
			MasterThread tmpThread = new MasterThread(m_listOfUsableHosts.get(i), command);
			listOfThreads.add(tmpThread);
		}
		
		/* Execution de la liste des threads */
		for(MasterThread th : listOfThreads)
			th.start();
		
		for(MasterThread th : listOfThreads)
			th.join();
		
		int endTime = (int) System.currentTimeMillis();
		System.out.println("Fin du reduce => Exécution en " + new Integer(endTime-startTime).toString() + "ms");
	}
	
	public void performResultGathering() throws IOException
	{
		System.out.println("\nDébut du gatheringResult");
		int startTime = (int) System.currentTimeMillis();
		
		/* Ouverture du fichier resultat */
		FileUtils.deleteQuietly( new File("/cal/homes/dutertre/INF_727/Shavadoop/result.txt") );
		File resultFile = new File("/cal/homes/dutertre/INF_727/Shavadoop/result.txt");
		
		ArrayList<String> outputText = new ArrayList<String>();
		
		/* Boucle sur les fichiers reducers */
		for(int i = 0; i<m_nbSlaveNodes; i++)
		{
			/* Ouverture du fichier RM correspondant et récupération de ses lignes */
			File currentRmFile = new File("/cal/homes/dutertre/INF_727/Shavadoop/RM" + new Integer(i).toString());
			List<String> lines = FileUtils.readLines(currentRmFile, new String("UTF-8"));
			
			/* Boucle sur les lignes de ce fichier pour ensuite les mettre dans le fichier result */
			for(String line : lines)
				outputText.add(line);
		}
		
		/* Ecriture */
		FileUtils.write(resultFile, StringUtils.join(outputText, "\n"), true);
		
		int endTime = (int) System.currentTimeMillis();
		System.out.println("Fin du gatheringResult => Exécution en " + new Integer(endTime-startTime).toString() + "ms");
	}
}
