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
package com.stratio.decision.shell.exception;

public class StreamingShellException extends RuntimeException {

    private static final long serialVersionUID = -6008162825488430717L;

    public StreamingShellException(String message) {
        super(message);
    }

    public StreamingShellException(Throwable cause) {
        super(cause);
    }

    @Override
    public String toString() {
        String error = "";
        if (getCause() != null) {
            error = getCause().getMessage();
        } else {
            error = getMessage();
        }
        return "-> ".concat(error);
    }
}
