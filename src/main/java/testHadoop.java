import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class testHadoop {
    public static void main(String[] args)
    {
        try{
            String filename = "hdfs://localhost:9000/input/file1.txt";
            Configuration conf = new Configuration();
            conf.addResource(new Path("/Users/shiyuan/hadoop-2.6.5/etc/hadoop/core-site.xml"));
            conf.addResource(new Path("/Users/shiyuan/hadoop-2.6.5/etc/hadoop/hdfs-site.xml"));
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(new Path(filename)))
            {
                System.out.println("文件存在！");
            }
            else
            {
                System.out.println("文件不存在！");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
