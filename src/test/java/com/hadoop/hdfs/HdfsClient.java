package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
}
