import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class Mapper 
{
	public static void main(String[] args) throws IOException
	{
		/* Récupération du numéro du thread ainsi que du nombre de reducers */
		int mapperIndex = Integer.parseInt(args[0]);
		int nbReducers = Integer.parseInt(args[1]);
		
		/* Initialisation des arrayLists */
		ArrayList< ArrayList<String> > linesOfOutputFiles = new ArrayList< ArrayList<String> >();
		for (int i = 0; i < nbReducers; i++) 
			linesOfOutputFiles.add( new ArrayList<String>() );
		
		/* Ouverture de son fichier split et récupération de ses lignes */
		File splitFile = new File("/cal/homes/dutertre/INF_727/Shavadoop/S" + new Integer(mapperIndex).toString());
		List<String> lines = FileUtils.readLines(splitFile, new String("UTF-8"));
		
		/* Création du fichier qui va contenir les clés */
		String keysFilename = "/cal/homes/dutertre/INF_727/Shavadoop/Keys" + new Integer(mapperIndex).toString();
		FileUtils.deleteQuietly( new File(keysFilename) );
		File keysFile = new File(keysFilename);
		
		/* Set qui va stocker les clés rencontrées */
		HashSet<String> keys = new HashSet<String>();
		
		/* Boucle sur les lignes du fichier */
		for(String line : lines)
		{
			/* Pour chaque mot de la ligne on indique dans le UM qu'il est présent */
			String[] words = line.split(" ");
			for(String word : words)
			{
				linesOfOutputFiles.get( Math.abs(word.hashCode())%nbReducers ).add(word + " 1");
				keys.add(word);
			}		
		}
		
		/* Création des fichiers UM relatif au thread et au bon reducer */
		for (int i = 0; i < nbReducers; i++) 
		{
			String umFilename = "/cal/homes/dutertre/INF_727/Shavadoop/UM" + new Integer(mapperIndex).toString() + "R" + new Integer(i).toString();
			FileUtils.deleteQuietly( new File(umFilename) );
			File umFile = new File(umFilename);
			String outputText = StringUtils.join(linesOfOutputFiles.get(i), "\n");
			FileUtils.write(umFile, outputText);
		}
		
		/* Ecriture des clés */
		String keysText = StringUtils.join(keys, "\n");
		FileUtils.write(keysFile, keysText);
	}
}
