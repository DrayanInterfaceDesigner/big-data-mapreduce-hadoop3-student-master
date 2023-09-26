package marseloddois;
import java.io.File;

public class FolderCounter {
    private String prefix;
    private String directory;
    public FolderCounter(String dir, String prefix) {
        this.prefix = prefix;
        this.directory = dir;
    }
    public int count() {
        // Directory path to search for folders
        String directoryPath = this.directory;

        // Prefix to search for
        String prefix = this.prefix;

        // Create a File object representing the directory
        File directory = new File(directoryPath);

        // Get a list of all files and directories in the directory
        File[] files = directory.listFiles();

        if (files != null) {
            int folderCount = 0;

            // Iterate through the files and count folders that start with the specified prefix
            for (File file : files) {
                if (file.isDirectory() && file.getName().startsWith(prefix)) {
                    folderCount++;
                }
            }
            return folderCount;
        } else {
            System.err.println("Directory not found or is empty.");
            return 0;
        }
    }
}
