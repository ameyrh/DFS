package DbaseAPI;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static DbaseAPI.file.*;

public class Segmentation {

    // the variable that get the current directory
    private static final String dir = System.getProperty("user.dir") +
            System.getProperty("file.separator");
    // suffix splitpart is writen as part of file name
    // for the programmer to under stand that its a segment of original file
    private static final String suffix = ".splitPart";
    static String iNode = null;
    static String nameOfFile = null;

    public static void start(byte[] filePlusKey,String path) throws IOException {
        //new File(dir+"upload").mkdir();
        // Create a path where the byte array filepluskey will be  written for
        // performing segmentation.
        // the addition of DFS2 ensures the existing file is not overwritten
        String writePath = path+ "DFS2";
        writeData(filePlusKey,writePath);
        // call the method splitFile with original path which is used for indexing
        // temporary path writePath from where the segmentation will take place
        // and size of each segment in KB
        splitFile(path,writePath,512);
    }
    /**
     * Split a file into multiples files.
     *
     * @param tempPath  Name of file to be split.
     * @param kbPerSplit number of kilo bytes per chunk.
     * @throws IOException
     */
    public static void splitFile(String inode,final String tempPath,final int kbPerSplit) throws IOException {

        File f;
        f = new File(inode);
        // get name of the file containing file plus key from the disk
        nameOfFile = f.getName();
        // get the inode
        iNode = inode;
        // enforce condition for the chunk size to be more than 0
        if (kbPerSplit <= 0)
            throw new IllegalArgumentException("chunkSize must be more than zero");
        // create an array list of Paths for the segments
        List<Path> partFiles = new ArrayList<>();
        // get the size of the file to be divided into segments
        final long sourceSize = Files.size(Paths.get(tempPath));
        // bytes per segment (convert KB to Bytes)
        final long bytesPerSplit = 1024L * kbPerSplit;
        // number of splits ( Total size divide by segment size)
        final long numSplits = sourceSize / bytesPerSplit;
        // remainder after the above division
        final long remainingBytes = sourceSize % bytesPerSplit;
        int position = 0;
        // create a file channel and access the file in read mode
        try (RandomAccessFile sourceFile = new RandomAccessFile(tempPath, "r");
             FileChannel sourceChannel = sourceFile.getChannel()) {
            // the loop traverses the channel using position
            // position is multiplied with number of bytes per segment every time
            for (; position < numSplits; position++)
                //write the content to different segments
                writePartToFile(bytesPerSplit, position * bytesPerSplit, sourceChannel, partFiles);
            // if some bytes are remaining after the whole division
            // write them as well to the segments
            if (remainingBytes > 0)
                writePartToFile(remainingBytes, position * bytesPerSplit, sourceChannel, partFiles);
        }
        //Delete the temporary encrypted file
        f = new File(tempPath);
        f.delete();
        //return partFiles;
    }
    // this method makes the segment to be written to the disk
    // it receives the channel and traverses the channel
    // writes the segments with unique name ( name of file followed by suffix .splitpart
    // followed by an integer) example xyz.splitpart.1
    private static void writePartToFile(long byteSize, long position, FileChannel sourceChannel,
                                        List<Path> partFiles) throws IOException {
        // path for the segment current directory followed by the inode followedby .splitpart
        // followed by the segment number
        Path segmentName = Paths.get(dir + nameOfFile+suffix + (int) ((position / (512 * 1024)) + 1));//TODO - replace the UUID with Integer.toString((position/512) - 1))
        try (RandomAccessFile toFile = new RandomAccessFile(segmentName.toFile(), "rw");
            FileChannel toChannel = toFile.getChannel()) {
            sourceChannel.position(position);
            toChannel.transferFrom(sourceChannel, 0, byteSize);
        }
        // add the segment name to the list
        partFiles.add(segmentName);
        // create index of the segments created with inode as the primary key
        splitIndex(iNode, segmentName.toString());
    }
    /**
     * Split a file into multiples files.
     *
     * @param inode Name of file to be split.
     * @param segmentName number of kilo bytes per chunk.
     * @throws IOException
     */
    public static void splitIndex(String inode, String segmentName){
        HashMap<String,String> index = new HashMap<>();
        // Put elements to the map
        index.put(inode, segmentName);// Put elements to the map
        String fileName = "SplitIndex.csv";
        /* Write CSV */
        try {
            String uploadPath = System.getProperty("user.dir") +
                    System.getProperty("file.separator") + fileName;
            // true is for appending and false is for over writing
            FileWriter writer = new FileWriter(uploadPath, true);
            Set set = index.entrySet();
            // Get an iterator for entering the data from hash map
            // to csv file
            for (Object o : set) {
                Map.Entry firstEntry = (Map.Entry) o;
                // write the key
                writer.write(firstEntry.getKey().toString());
                // write the comma
                writer.write(",");//Explore how to write key and value in different fields
                // write the value against key
                writer.write(firstEntry.getValue().toString());
                // create a new line
                writer.write("\n");
            }
            // close the writer
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
