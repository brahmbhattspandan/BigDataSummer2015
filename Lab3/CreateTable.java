import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;

public class CreateTable {

    public static void main(String[] args) throws IOException {

        // Instantiating configuration class
        Configuration con = HBaseConfiguration.create();

        // Instantiating HbaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(con);

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new
                HTableDescriptor(TableName.valueOf("emp1"));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));

        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");

        // Getting all the list of tables using HBaseAdmin object
        HTableDescriptor[] listtableDescriptor = admin.listTables();

        // printing all the table names.
        for (int i=0; i<listtableDescriptor.length;i++ ){
            System.out.println(listtableDescriptor[i].getNameAsString());
        }

        // Instantiating columnDescriptor class
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("contactDetails");

        // Adding column family
        admin.addColumn("emp1", columnDescriptor);
        System.out.println("Column added");

        // Deleting a column family
        admin.deleteColumn("emp1", "contactDetails");
        System.out.println("Column deleted");

        // disabling table named emp
        admin.disableTable("emp1");

        // Deleting emp
        admin.deleteTable("emp1");
        System.out.println("Table deleted");
    }
}