package com.salesforce.phoenix.arithmetic;

import java.math.BigDecimal;

import com.salesforce.phoenix.schema.PDataType;

public class DecimalSumTest {

    private final static byte[][] rawBytesValues = new byte[][] {
        new byte[] {-63, 25, 18, 1, 1, 1, 1, 1, 1, 18},
        new byte[] {-63, 72, 43, 80, 100, 100, 100, 100, 100, 74},
        new byte[] {-63, 70, 42, 11, 1, 1, 1, 1, 1, 15},
        new byte[] {-63, 90, 75, 30, 100, 100, 100, 100, 100, 51},
        new byte[] {-63, 78, 5, 91, 1, 1, 1, 1, 1, 67},
        new byte[] {-63, 57, 26},
        new byte[] {-63, 42, 67, 60, 100, 100, 100, 100, 100, 69},
        new byte[] {-63, 34, 34, 30, 100, 100, 100, 100, 100, 85},
        new byte[] {-62, 2},
        new byte[] {-63, 71, 59, 80, 100, 100, 100, 100, 100, 40},
        new byte[] {-63, 26},
        new byte[] {-128},
        new byte[] {-63, 84, 34, 30, 100, 100, 100, 100, 100, 85},
        new byte[] {-63, 62, 29, 100, 100, 100, 100, 100, 100, 92},
        new byte[] {-63, 28, 59, 60, 100, 100, 100, 100, 100, 86},
        new byte[] {-63, 76},
        new byte[] {-63, 95, 74, 61, 1, 1, 1, 1, 1, 43}};
    
    
}
