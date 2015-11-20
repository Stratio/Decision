/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.exception;

public class StreamExistsException extends RequestValidationException {

    private static final long serialVersionUID = 1191962540939323834L;

    private final int code;

    public StreamExistsException(int code, String message, Throwable cause) {
        super(code, message, cause);
        this.code = code;
    }

    public StreamExistsException(int code, String message) {
        super(code, message);
        this.code = code;
    }

    public StreamExistsException(int code, Throwable cause) {
        super(code, cause);
        this.code = code;
    }

}
