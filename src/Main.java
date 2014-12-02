import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;

import org.apache.log4j.BasicConfigurator;

public class Main 
{
	public static void main(String[] args) throws IOException, InterruptedException 
	{		
		/* Paramètres du Shavadoop si l'utilsateur n'en spécifie pas */
		String logFilename = "/cal/homes/dutertre/INF_727/Shavadoop/log.txt";
		String inputFilename = "/cal/homes/dutertre/INF_727/Shavadoop/60Bibles.txt";
		String hostsFilename = "/cal/homes/dutertre/INF_727/Shavadoop/listeMachines.txt";
		int nbWorkers = 10;
		
		/* Si l'utilsateur a spécifié un fichier et un nombre de workers */
		if( args.length == 2 )
		{
			inputFilename = args[0];
			nbWorkers = Integer.parseInt( args[1] );
		}
			
		
		/* Redirection des erreurs dans un fichier pour en garder une trace et ne pas polluer l'affichage console */
		System.setErr(new PrintStream(new File(logFilename)));
		
		/* Initialisation du chrono */
		System.out.println("--------------------------------------------------");
		System.out.println("\tDébut du wordCount en Shavadoop");
		System.out.println("--------------------------------------------------");
		int startTime = (int) System.currentTimeMillis();
		
		/* Affichage de la config courante */
		System.out.println("\nFichier à traiter : " + inputFilename);
		System.out.println("Nombre de workers : " + new Integer(nbWorkers).toString());
		
		/* Exécution du wordcount par Shavadoop */
		MasterNode masterNode = new MasterNode(inputFilename, hostsFilename, nbWorkers);
		masterNode.splitInputFile();
		masterNode.performMapping();
		masterNode.performShuffle(); 	
		masterNode.performReduce();
		masterNode.performResultGathering();
		
		/* Arrêt du chrono */
		int endTime = (int) System.currentTimeMillis();
		System.out.println("\n=> Temps total d'exécution : " + new Integer(endTime-startTime).toString() + "ms <=");
		
		System.out.println("\n--------------------------------------------------");
		System.out.println("\tFin du wordCount en Shavadoop");
		System.out.println("--------------------------------------------------");
	}
}