import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class Reducer 
{
	public static void main(String[] args) throws IOException
	{
		/* Récupération du numéro du thread */
		int reducerIndex = Integer.parseInt(args[0]);
		
		/* Ouverture de son fichier SM et récupération de ses lignes */
		File smFile = new File("/cal/homes/dutertre/INF_727/Shavadoop/SM" + new Integer(reducerIndex).toString());
		List<String> lines = FileUtils.readLines(smFile, new String("UTF-8"));
		
		/* Boucle sur les lignes du fichier */
		ArrayList<String> outputText = new ArrayList<String>();
		
		for(String line : lines)
		{
			/* Récupération de la clé et de la liste des 1 */
			String[] words = line.split(" ");
			String key = words[0];
			String[] listOfOnes = words[1].split(";");
			int keyOccurences = 0;
			
			/* Calcul du nombre d'occurences de la clé */
			for(String one : listOfOnes)
				keyOccurences += Integer.parseInt(one);
			
			/* On a la clé et son nombre d'occurences => on peut donc les écrire */
			outputText.add( key + " " + new Integer(keyOccurences).toString() );
		}
		
		/* Création du fichier d'output RM relatif au thread */
		String rmFilename = "/cal/homes/dutertre/INF_727/Shavadoop/RM" + new Integer(reducerIndex).toString();
		FileUtils.deleteQuietly( new File(rmFilename) );
		File rmFile = new File(rmFilename);
		FileUtils.write(rmFile, StringUtils.join(outputText, "\n"));
	}
}
