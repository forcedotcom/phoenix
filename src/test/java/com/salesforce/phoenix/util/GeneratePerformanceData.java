/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

public class GeneratePerformanceData {
    private static final String FILENAME = "data.csv";

    public static void main(String[] args) throws FileNotFoundException, IOException {
        String[] host = {"NA","CS","EU"};
        String[] domain = {"Salesforce.com","Apple.com","Google.com"};
        String[] feature = {"Login","Report","Dashboard"};
        Calendar now = GregorianCalendar.getInstance();
        FileOutputStream fostream = new FileOutputStream(FILENAME);
        try {
            Random random = new Random();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if (args.length < 1) {
                System.out.println("Row count must be specified as argument");
                return;
            }
            int rowCount = Integer.parseInt(args[0]);
            for (int i=0; i<rowCount; i++) {
                now.add(Calendar.SECOND, 1);
                fostream.write((host[random.nextInt(host.length)] + "," + 
                        domain[random.nextInt(domain.length)] + "," + 
                        feature[random.nextInt(feature.length)] + "," + 
                        sdf.format(now.getTime()) + "," + 
                        random.nextInt(500) + "," + 
                        random.nextInt(2000)+"," + 
                        random.nextInt(10000) + 
                        "\n").getBytes());
                if (i % 10000 == 0) {
                    System.out.print(".");
                }
            }
        } finally {
            fostream.close();
        }
    }
}
