/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.trino.storage;

import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;

import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.ResponseHandlerUtils.readResponseBytes;

public class ByteResponseHandler
        implements ResponseHandler<ByteResponseHandler.ByteResponse, RuntimeException>
{
    private static final ByteResponseHandler BYTE_RESPONSE_HANDLER = new ByteResponseHandler();

    public static ByteResponseHandler createByteResponseHandler()
    {
        return BYTE_RESPONSE_HANDLER;
    }

    private ByteResponseHandler() {}

    @Override
    public ByteResponse handleException(Request request, Exception exception)
    {
        throw propagate(request, exception);
    }

    @Override
    public ByteResponse handle(Request request, Response response)
    {
        byte[] bytes = readResponseBytes(request, response);
        return new ByteResponse(response.getStatusCode(), bytes);
    }

    public static class ByteResponse
    {
        private final int statusCode;
        private final byte[] body;

        public ByteResponse(int statusCode, byte[] body)
        {
            this.statusCode = statusCode;
            this.body = body;
        }

        public int getStatusCode()
        {
            return statusCode;
        }

        public byte[] getBody()
        {
            return body;
        }
    }
}
