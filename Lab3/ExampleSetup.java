import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.Random;

/**
 * Helper class that inserts data for us to analyze
 */
public class ExampleSetup
{
    public static MessageDigest md5;
    public static Random random = new Random();
    static {
        try {
            // Create an MD5 Hash object
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    /**
     * Generates a row key that contains an MD5 hash of the specified user ID and a random date in the year 2014 but
     * with a random month, day, and time
     *
     * @param userId    The user ID for which to generate the row key
     *
     * @return          A byte[] that contains the row key
     */
    public static byte[] generateRowKey( String userId )
    {
        // Create an MD5 Hash of the User ID
        byte[] userIdHash = md5.digest( userId.getBytes() );

        // Generate a random timestamp
        Calendar calendar = Calendar.getInstance();
        calendar.set( Calendar.YEAR, 2014 );
        calendar.set( Calendar.MONTH, random.nextInt( 12 ) );
        calendar.set( Calendar.DAY_OF_MONTH, random.nextInt( 27 ) + 1 );
        calendar.set( Calendar.HOUR_OF_DAY, random.nextInt( 24 ) );
        calendar.set( Calendar.MINUTE, random.nextInt( 60 ) );
        calendar.set( Calendar.SECOND, random.nextInt( 60 ) );
        byte[] timestamp = Bytes.toBytes(calendar.getTimeInMillis());

        // 16 bytes for MD5 length + size of a Long
        byte[] rowkey = new byte[ 16 + Long.SIZE/8 ];
        int offset = 0;
        offset = Bytes.putBytes( rowkey, offset, userIdHash, 0, userIdHash.length );
        Bytes.putBytes( rowkey, offset, timestamp, 0, timestamp.length );

        return rowkey;
    }

    /**
     * Extracts the date from a row key. Note that the user ID is hashed (one-way operation) so there is no way
     * to retrieve the user ID, but you can retrieve the date
     *
     * @param rowkey        The row key from which to retrieve the time stamp
     *
     * @return              The timestamp as a long
     */
    public static long getTimestampFromRowKey( byte[] rowkey )
    {
        try
        {
            // Extract the time stamp bytes
            byte[] timestampBytes = Bytes.copy(rowkey, 16, Long.SIZE / 8);

            // Convert the byte[] to a long
            ByteArrayInputStream bais = new ByteArrayInputStream(timestampBytes);
            DataInputStream dis = new DataInputStream(bais);
            return dis.readLong();
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }

        // The operation failed
        return -1;
    }

    public static void main( String[] args )
    {
        try
        {

            // Create a configuration that connects to a local HBase
            Configuration conf = HBaseConfiguration.create();

            // Connect to the PageViews table
            HTableInterface pageViewTable = new HTable( conf, "PageViews" );

            // Insert 10,000 rows
            for( int i=0; i<100; i++ )

            {

                // Create a user ID for this user: the user ID will be User 0, User 1, ... User 99
                String userId = "User " + i%100;

                // Create a Put object for this row key
                Put put = new Put( generateRowKey( userId ) );

                // Add the user id to the info column family
                put.add( Bytes.toBytes( "info" ),
                        Bytes.toBytes( "userId" ),
                        Bytes.toBytes( userId ) );

                // Add the page to the info column family
                put.add( Bytes.toBytes( "info" ),
                        Bytes.toBytes( "page" ),
                        Bytes.toBytes( "/page/" + i%100 ) );

                // Add the PageView to the page view table
                pageViewTable.put( put );

            }

            // Close the connection to the table
            pageViewTable.close();

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}