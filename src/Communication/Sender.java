package Communication;

import java.io.*;
import java.net.Socket;

public class Sender {
    public static void start(String xmlPath, String IP) throws IOException {
        Socket socket = null;
        socket = new Socket(IP, 4444);

        File file = new File(xmlPath);
        // Get the size of the file
        long length = file.length();
        byte[] bytes = new byte[16 * 1024];
        InputStream in = new FileInputStream(file);
        //BufferedInputStream bis = new BufferedInputStream(in);
        //DataInputStream dis = new DataInputStream(bis);

        OutputStream out = socket.getOutputStream();

        int count;
        while ((count = in.read(bytes)) > 0) {
            out.write(bytes, 0, count);
        }
        out.close();
        in.close();
        socket.close();
    }
}
