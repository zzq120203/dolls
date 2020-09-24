package com.zzq.dolls.zhconverter;

import org.junit.Test;

    
public class TestZHConverter {

    @Test
    public void testZh() {
        ZHConverter converter = ZHConverter.getInstance(ZHConverter.TRADITIONAL);
        String s = converter.convert("干活我们干系我的");

        System.out.println(s);
    }
}
