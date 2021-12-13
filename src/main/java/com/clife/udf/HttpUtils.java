package com.clife.udf;

import org.apache.commons.codec.binary.Base64;


import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtils {

    public static final String dirPath = "C:\\Users\\86000014\\Desktop\\hbasetosparktest";
    public static final String picUrl = "https://skinsecret.clife.cn/clife/tencent/2021.05.28_21.18.49.551_4401BBE45B30.live";
    public static final String picName = picUrl.substring(picUrl.lastIndexOf("/"));
    public static byte[] b1 = null;
    public static byte[] b2 = null;
    /**
     * main方法测试
     *
     * @String args
     */
    public static void main(String[] args) {
        try {
            //这个地址你们是访问不到，因为网络限制，测试的话放自己可以访问的地址哦
            downLoadFromUrl(picUrl, picName, dirPath);
            readFile(dirPath + "\\" + picName);

            if (b1.length == b2.length){
                for (int i = 0; i < b1.length ; i++){
                    if (b1[i] != b2[i]){
                        throw new Exception("sadffffffff");
                    }
                }
            }




        } catch (Exception e) {
            e.printStackTrace();
            // TODO: handle exception
        }
    }

    /**
     * 根据Url下载url中的附件
     *
     * @param urlStr
     * @param fileName
     * @param savePath
     * @throws IOException
     */
    public static void downLoadFromUrl(String urlStr, String fileName, String savePath) throws IOException {
        //确定自己的url地址
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();  //连接
        //设置超时间为3秒
        conn.setConnectTimeout(30 * 1000);
        //防止屏蔽程序抓取而返回403错误
        conn.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 5.0;Windows NT; DigExt)");
        //得到输入流
        InputStream inputStream = conn.getInputStream();
        //获取自己数组（方法在下面）
        byte[] getData = readInputStream(inputStream);
        b1 = readInputStream(inputStream);
        //文件保存位置（文件下载后的位置）
        File saveDir = new File(savePath);
        if (!saveDir.exists()) {
            saveDir.mkdir();
        }
        File file = new File(saveDir + File.separator + fileName);
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(getData);
        if (fos != null) {
            fos.close();
        }
        if (inputStream != null) {
            inputStream.close();
        }
        //打印一下，顺便作为文件成功下载的标记
        System.out.println("info:" + url + " download success");
    }


    /**
     * 从输入流中获取字节数组
     *
     * @param inputStream
     * @return
     * @throws IOException
     */
    public static byte[] readInputStream(InputStream inputStream) throws IOException {
        byte[] buffer = new byte[1024];
        int len = 0;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((len = inputStream.read(buffer)) != -1) {
            bos.write(buffer, 0, len);
        }
        bos.close();
        return bos.toByteArray();
    }



    public static  void readFile(String filename) throws Exception {
        try {
//实体自定义
            File file = new File(filename);
//字节流转换赋值
            byte[] data = getBytesFromFile(file);
            b2 = getBytesFromFile(file);
//插库操作具体方法不在赘述
            System.out.println("asdf");
            String s = new String(Base64.encodeBase64(data));
            System.out.println(new String(Base64.encodeBase64(data)));
//String Jbnr=getBytesFromFile(file)；
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] getBytesFromFile(File f) {
        if (f == null) {
            return null;
        }
        try {
//开通文件输入输出流
            FileInputStream stream = new FileInputStream(f);
            ByteArrayOutputStream out = new
                    ByteArrayOutputStream();
            byte[] b = new byte[1000];
            int n;
            while ((n = stream.read(b)) != -1) {
                out.write(b, 0, n);
            }
            stream.close();
            out.close();

//转换后的二进制数据
            return out.toByteArray();
        } catch (IOException e) {

        }
        return null;
    }




    public void download(String url) {
// 设置文件对象，给定本地相对路径
        File file = new File("/opt/bingege/file");
        FileOutputStream fop = null;
        InputStream in = null;
        try {
//设置输出流 要写入的文件
            fop = new FileOutputStream(file);
//给定 URL 文件路径,获取文件输入流
            in = new FileInputStream(new File(url));
            byte[] buffer = new byte[1024];

            int len = -1;

            while ((len = in.read(buffer)) != -1) {

                fop.write(buffer, 0, len);
            }
            fop.flush();
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (fop != null) {
                    fop.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
