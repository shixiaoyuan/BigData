package Hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class hbExec {
    public hbExec(){}
    public static Configuration configuration;
    public static Connection connection;
    public static  Admin admin;

    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = new HBaseAdmin(configuration);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    //关闭数据库
    public static void close(){
        try{
            if(admin!=null)
                admin.close();
            if(connection!=null)
                connection.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    //建表
    public static void createTable(String myTableName, String[] colFamily) throws IOException{
        init();
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName))
            System.out.println("table exists");
        else{
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String str:colFamily)
            {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        close();
    }
    //插入数据
    public static void insertData(String tableName, String rowkey, String colFamily, String col, String val) throws IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);
        table.close();
        close();
    }
    //查找数据
    public static void getData(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        Result result = table.get(get);
        System.out.println(new String(result.getValue(colFamily.getBytes(),col==null?null:col.getBytes())));
        table.close();
        close();
    }
}
