import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/* Cette classe devra répondre aux spécifications suivantes :
 *  -Constructeur à partir du nom du fichier et du mode d'accès (lecture et écriture)
 *  -Méthode getNextLine qui renvoie la ligne suivante du fichier (String) ou null si c'est impossible
 *  -Méthode getAllFileContent qui renvoie un String représentant tout le texte du fichier et null sinon
 */
public class FileHandler 
{
	/* Ouverture du fichier en mode lecture */
	public FileHandler(String filepath)
	{
		m_filepath = filepath;
		
		try 
		{
			m_bufferReader = new BufferedReader(new FileReader(m_filepath));
		} 
		catch (FileNotFoundException e) 
		{
			System.out.println("Erreur lors de l'ouverture du fichier : " + e.getMessage());
		}
	}
	
	/* Méthode getNextLine */
	public String getNextLine() throws IOException
	{	
		String nextLine = m_bufferReader.readLine();
		
		if( nextLine == null )
			m_bufferReader.close();
		
		return nextLine;
	}
	
	/* Méthode getAllFileContent */
	public String getAllFileContent() throws IOException
	{		
		File file = new File(m_filepath);
	    FileInputStream fis = new FileInputStream(file);
	    byte[] data = new byte[(int)file.length()];
	    fis.read(data);
	    fis.close();
	    return new String(data, "UTF-8");
	}
	
	private String m_filepath;
	private BufferedReader m_bufferReader;
}
