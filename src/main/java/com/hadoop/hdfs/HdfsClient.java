package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接的集群nn地址
        URI uri = new URI("hdfs://hadoop101:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();
        //设置副本数
        configuration.set("dfs.replication","2");
        //用户
        String user = "root";
        //获取的客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        //关闭资源
        fs.close();
    }

    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        //创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    @Test
    public void testPut() throws IOException {
        /**
         * 文件上传
         * 参数一：删除原数据
         * 参数二：是否允许覆盖
         * 参数三：原数据路径
         * 参数四：目的地路径
         */
        fs.copyFromLocalFile(false
                ,true,
                new Path("D:\\sunwukong.txt"),
                new Path("hdfs://hadoop101/xiyou/huaguoshan"));
    }

    @Test
    public void testGet() throws IOException {
        /**
         * 文件下载
         * 参数一：原文件是否删除
         * 参数二：原文件HDFS路径
         * 参数三：目标地址路径
         * 参数四：是否使用本地原文件系统
         */
        fs.copyToLocalFile(false,
                new Path("hdfs://hadoop101/xiyou/huaguoshan/sunwukong.txt"),
                new Path("D:\\"),
                true);
    }

    @Test
    public void testRm() throws IOException {
        /**
         * 删除
         * 参数一：要删除的路径
         * 参数二：是否递归删除
         */
        //删除文件
        fs.delete(new Path("/jdk-8u351-linux-x64.tar.gz"),false);

        //删除空目录
        fs.delete(new Path("/xiyou"),false);

        //删除非空目录
        fs.delete(new Path("/jinguo"),true);
    }

    @Test
    public void testmv() throws IOException {
        /**
         * 参数一：原文件路径
         * 参数二：目标文件路径
         */
        //修改文件名称
//        fs.rename(new Path("/wcinput/word.txt"),new Path("/wcinput/ss.txt"));

        //文件的移动和更名
//        fs.rename(new Path("/wcinput/ss.txt"),new Path("/ssyy.txt"));

        //目录更名
        fs.rename(new Path("/wcinput"),new Path("/input"));
    }

    @Test
    public void fileDetail() throws IOException {
        //获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========="+fileStatus.getPath()+"==========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    //判断是文件还是目录
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status:listStatus) {
            if (status.isFile()) {
                System.out.println("文件:"+status.getPath().getName());
            }else {
                System.out.println("目录:"+status.getPath().getName());
            }
        }
    }

}
