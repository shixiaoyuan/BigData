package Hbase;
import java.io.IOException;

public class testhb {
    public static void main(String[] args){
        try{
            String[] family = {"core"};

            hbExec.createTable("student",family);
            hbExec.insertData("student","liujing","core","English", "98");
            hbExec.insertData("student","liujing","core","Chinese", "97");
            hbExec.getData("student","liujing","core","English");
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
