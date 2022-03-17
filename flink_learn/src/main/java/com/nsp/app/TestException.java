package com.nsp.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestException {

    private static final Logger LOGGER = LoggerFactory.getLogger("app");

    public static void main(String[] args) {
        try {
            int i = 10 / 0;
        } catch (Exception e) {
            LOGGER.error("occur e:", e);
        }
    }
}
