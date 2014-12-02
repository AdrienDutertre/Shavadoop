import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class Shuffler 
{
	public static void main(String[] args) throws IOException, InterruptedException
	{	
		/* Récupération du numéro de ce thread et du nombre de mappers */
		int shufflerIndex = Integer.parseInt(args[0]);
		int nbMappers = Integer.parseInt(args[1]);
		
		HashMap<String, ArrayList<String>> shuffleOutput = new HashMap<String, ArrayList<String>>();
		
		/* Boucle sur tous les fichiers UMiRj */
		for(int i=0; i<nbMappers; i++)
		{
			/* Ouverture de ce fichier et récupération de ses lignes */
			String filename = "/cal/homes/dutertre/INF_727/Shavadoop/UM" + i + "R" + shufflerIndex;
			List<String> linesOfCurrentFiles = FileUtils.readLines( new File(filename), "utf-8");
			
			/* Boucle sur le contenu du fichier courant */
			for(String line : linesOfCurrentFiles)
			{
				/* Extraction de la clé et de sa valeur */ 
				String[] splits = line.split(" ");
				
				/* Insertion dans la hasmap globale en dissociant le cas ou on a déja vu cette clé ou non */ 
				if( shuffleOutput.containsKey(splits[0]) )
					shuffleOutput.get(splits[0]).add(splits[1]);
				else
				{
					ArrayList<String> tmp = new ArrayList<String>();
					tmp.add(splits[1]);
					shuffleOutput.put(splits[0], tmp);
				}
			}
		}
		
		/* Conversion de la hashmap en un string global */
		ArrayList<String> text = new ArrayList<>();
		for(String key : shuffleOutput.keySet())
			text.add( key + " " + StringUtils.join(shuffleOutput.get(key), ";") );
		
		/* Création, ouverture et écriture du fichier d'output */
		String smFilename = "/cal/homes/dutertre/INF_727/Shavadoop/SM" + new Integer(shufflerIndex).toString();
		FileUtils.deleteQuietly( new File(smFilename) );
		File smFile = new File(smFilename);
		String allText = StringUtils.join(text, "\n");
		FileUtils.write(smFile, allText);
	}
}
