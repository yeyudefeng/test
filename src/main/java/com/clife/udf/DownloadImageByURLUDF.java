package com.clife.udf;

import org.apache.commons.codec.binary.Base64;

import org.apache.commons.lang3.SystemUtils;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.*;
import java.net.HttpURLConnection;

import java.net.URL;


public class DownloadImageByURLUDF extends UDF {
    public static final String dirPath = SystemUtils.IS_OS_WINDOWS ? "C:\\Users\\86000014\\Desktop\\hbasetosparktest\\" : "/tmp/infrared_detectors/pic/";

    static  {
        File file = new File(dirPath);
        boolean exists = file.exists();
        if (!exists){
            file.mkdirs();
            System.out.println("创建目录成功 ：" + dirPath);
        }
    }
    public static void main(String[] args) throws Exception {
        new DownloadImageByURLUDF().evaluate("https://skinsecret.clife.cn/clife/tencent/2021.05.28_21.18.49.551_4401BBE45B30.live");
    }
    public String evaluate(final String url) throws Exception {
        if (url == null) {
            return null;
        }
        String picturePath = null;
        try {
            String pictureName = url.substring(url.lastIndexOf("/") + 1);
            downLoadFromUrl(url, pictureName, dirPath);
            picturePath = dirPath + pictureName;

            byte[] data = readFile(picturePath);
            return new String(Base64.encodeBase64(data));
//        } catch (Exception e){
//            return null;
        } finally {
            deletePicture(picturePath);
        }
    }

    public void deletePicture(String picturePath){
        if (picturePath == null){
            return;
        }
//        try {
            new File(picturePath).delete();
//        } catch (Exception e){
//
//        }
    }


    /**
     * 根据Url下载url中的附件
     *
     * @param urlStr
     * @param fileName
     * @param savePath
     * @throws IOException
     */
    public void downLoadFromUrl(String urlStr, String fileName, String savePath) throws IOException {
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
//        //打印一下，顺便作为文件成功下载的标记
//        System.out.println("info:" + url + " download success");
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


    public static byte[] readFile(String filename) throws Exception {
        try {
            File file = new File(filename);
            //字节流转换赋值
            byte[] data = getBytesFromFile(file);
//            System.out.println(new String(data,"GBK"));
//            System.out.println(new String(Base64.encodeBase64(data)));
            return data;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] getBytesFromFile(File f) {
        if (f == null) {
            return null;
        }
        try {
            //开通文件输入输出流
            FileInputStream stream = new FileInputStream(f);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
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
}
