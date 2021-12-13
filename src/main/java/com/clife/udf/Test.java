package com.clife.udf;

/**
 * Java中ASC码与字符互相转化
 *
 * @author jiqinlin
 *
 */
public class Test {
    private static int ascNum;
    private static char strChar;

    public static void main(String[] args) {
        System.out.println(getAsc("a"));
        System.out.println(backchar(98));
        String s = hexStringToString("\\x00\\x00\\x00\\x00\\x00\\x00+\\x1F\\x00\\x00\\x013\\xA4Y\\xDB\\xC8");
        String s1 = removeFourChar("\\x00\\x00\\x00\\x00\\x00\\x00+\\x1F\\x00\\x00\\x013\\xA4Y\\xDB\\xC8");
        System.out.println(s1);
        System.out.println(s);
    }

    /**
     * 字符转ASC
     *
     * @param st
     * @return
     */
    public static int getAsc(String st) {
        byte[] gc = st.getBytes();
        ascNum = (int) gc[0];
        System.out.println(gc[0]);
        return ascNum;
    }

    /**
     * ASC转字符
     *
     * @param backnum
     * @return
     */
    public static char backchar(int backnum) {
        strChar = (char) backnum;
        return strChar;
    }

    /**
     * 16进制转换成为string类型字符串
     * @param s
     * @return
     */
    public static String hexStringToString(String s) {
        if (s == null || s.equals("")) {
            return null;
        }
        s = s.replace(" ", "");
        byte[] baKeyword = new byte[s.length() / 2];
        for (int i = 0; i < baKeyword.length; i++) {
            try {
                baKeyword[i] = (byte) (0xff & Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            s = new String(baKeyword, "UTF-8");
            new String();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return s;
    }

    /**
     * 替换四个字节的字符 '\xF0\x9F\x98\x84\xF0\x9F）的解决方案 😁
     * @author ChenGuiYong
     * @data 2015年8月11日 上午10:31:50
     * @param content
     * @return
     */
    public static String removeFourChar(String content) {
        byte[] conbyte = content.getBytes();
        for (int i = 0; i < conbyte.length; i++) {
            if ((conbyte[i] & 0xF8) == 0xF0) {
                for (int j = 0; j < 4; j++) {
                    conbyte[i+j]=0x30;
                }
                i += 3;
            }
        }
        content = new String(conbyte);
        return content.replaceAll("0000", "");
    }

}
