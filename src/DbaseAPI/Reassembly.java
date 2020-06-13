package DbaseAPI;

import DFS2.Download;
import Encryption.Encrypt;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

import static DbaseAPI.file.*;

public class Reassembly {

    private static final String dir = System.getProperty("user.dir") +
            System.getProperty("file.separator");
    public static void start(byte[] inbound,String fileName) throws IOException {
        // send the file for sequencing
        sequencing(inbound,fileName);
    }
    /**
     * Split a file into multiples files.
     *
     * @param fileName  Name of file to be split.
     * @param inbound number of kilo bytes per chunk.
     * @throws IOException
     */
    public static void sequencing(byte[] inbound,String fileName) throws IOException {

        int sequenceNo = 0;
        // parse the down loaded segements by Type, Length and value
        // here sequenced means the parsed byte array
        byte[] sequenced = TLVParser.startParsing(inbound, 4);
        // if three Zeros follow the value right at start then the value
        // is sequence number
        if(sequenced[1]==0 && sequenced[2]==0 &&sequenced[3]==0)
            sequenceNo = sequenced[0];
        //byte[] data = TLVParser.startParsing(sequenced,4);

        // after retrieving the sequence number retain only value
        // discard type and length
        byte[] data = deconcat(inbound,16);
        // access the splitIndex to retrieve the inode for the segment
        String splitFile = dir + "SplitIndex.csv";
        String[] segmentInode = csvreader(splitFile,fileName);
        // write the segmentdata to the segment inode
        writeData(data,segmentInode[sequenceNo-1]);
    }
    /**
     * Split a file into multiples files.
     *
     * @param fileName  Name of file to be split.
     * @param fileCount number of kilo bytes per chunk.
     * @throws IOException
     */
     public static void stichfiles (String fileName,int fileCount) throws IOException, GeneralSecurityException {

        String[] segmentInode = csvreader(dir + "SplitIndex.csv",fileName);
        // create a byte array where the all the segments will be merged
        byte[] completeFile = new byte[0];
        // loop runs till all the segments are addressed
        // the null condition is there as csv reader returns array with null values and
        // inodes both if this is fixed the condition can be removed
        for(int i =0;i<segmentInode.length  && !(segmentInode[i]==null);i++){
            // read data of each segment
            byte[] segmentData = readdata(segmentInode[i]);
            // concat the segment data to the completefile byte array
            completeFile = Encrypt.concat(completeFile,segmentData);
            // delete the segments once their data has been read and
            // concatenated
            File f;
            f = new File(segmentInode[i]);
            f.delete();
        }
        // pass the complete file byte array for post download processing
        // such as matching the hash and writing to disk
         Download.postDownload(completeFile);
    }
}
