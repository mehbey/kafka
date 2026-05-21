/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ListClientConnectionsRequestData;
import org.apache.kafka.common.message.ListClientConnectionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class ListClientConnectionsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListClientConnectionsRequest> {
        public final ListClientConnectionsRequestData data;

        public Builder(ListClientConnectionsRequestData data) {
            super(ApiKeys.LIST_CLIENT_CONNECTIONS);
            this.data = data;
        }

        @Override
        public ListClientConnectionsRequest build(short version) {
            return new ListClientConnectionsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListClientConnectionsRequestData data;

    private ListClientConnectionsRequest(ListClientConnectionsRequestData data, short version) {
        super(ApiKeys.LIST_CLIENT_CONNECTIONS, version);
        this.data = data;
    }

    public ListClientConnectionsRequestData data() {
        return data;
    }

    @Override
    public ListClientConnectionsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        ListClientConnectionsResponseData response = new ListClientConnectionsResponseData()
            .setErrorCode(error.code())
            .setThrottleTimeMs(throttleTimeMs);
        return new ListClientConnectionsResponse(response);
    }

    public static ListClientConnectionsRequest parse(Readable readable, short version) {
        return new ListClientConnectionsRequest(new ListClientConnectionsRequestData(
            readable, version), version);
    }

    @Override
    public String toString(boolean verbose) {
        return data.toString();
    }
}
