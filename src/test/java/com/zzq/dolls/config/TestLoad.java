package com.zzq.dolls.config;

import java.io.IOException;

import org.junit.Test;

public class TestLoad {

    @Test
    public void s() throws IOException {
        LoadConfig.load(TSMConf.class);

        System.out.println(LoadConfig.toString(TSMConf.class));
    }

    public static void main(String[] args) throws IOException {
        LoadConfig.load(TSMConf.class);

        System.out.println(LoadConfig.toString(TSMConf.class));
    }
}
