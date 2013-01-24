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
package phoenix.iterate;

import java.sql.SQLException;
import java.util.List;

import phoenix.execute.RowCounter;
import phoenix.schema.tuple.Tuple;

/**
 * 
 * Result scanner that wraps another result scanner and honors a row count limit.
 *
 * @author jtaylor
 * @since 0.1
 */
public class SerialLimitingResultIterator implements ResultIterator {
    private final ResultIterator scanner;
    private final long limit;
    private final RowCounter rowCounter;
    
    private long count;
    
    public SerialLimitingResultIterator(ResultIterator scanner, long limit, RowCounter rowCounter) {
        this.scanner = scanner;
        this.limit = limit;
        this.rowCounter = rowCounter;
    }

    @Override
    public void close() throws SQLException {
        scanner.close();
    }

    @Override
    public Tuple next() throws SQLException {
        if (count == limit) {
            return null;
        }
        Tuple result = scanner.next();
        if (result != null) {
            count += rowCounter.calculate(result);
        }
        return result;
    }

    @Override
    public void explain(List<String> planSteps) {
        scanner.explain(planSteps);
        StringBuilder buf = new StringBuilder("CLIENT SERIAL");
        if (rowCounter != RowCounter.UNLIMIT_ROW_COUNTER) {
            buf.append(" " + limit + " ROW LIMIT");
        }
        if (planSteps.isEmpty()) {
            planSteps.add(buf.toString());
        } else {
            planSteps.set(0, buf.toString() + " " + planSteps.get(0));
        }
    }
}
