package benchMark;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TestUtil {
    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static Random rnd = new Random();

    private static final NumberFormat MEM_FMT = new DecimalFormat("##,###.##");

    public static String randomString(int len)
    {
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }

    public static void main(String[] args) {
        byte[] tmpKey;
        byte[] tmpValue;
        Map<byte[], byte[]> kvs = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            tmpKey = TestUtil.randomString(8).getBytes();
            tmpValue = TestUtil.randomString(4000).getBytes();
            System.out.println(tmpKey.length + " ," + tmpValue.length);
            kvs.put(tmpKey, tmpValue);
        }
    }

}
