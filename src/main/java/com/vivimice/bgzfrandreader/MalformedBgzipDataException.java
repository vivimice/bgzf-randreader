package com.vivimice.bgzfrandreader;

import java.io.IOException;

public class MalformedBgzipDataException extends IOException {

    private static final long serialVersionUID = 7190259543765782955L;

    public MalformedBgzipDataException() {
        super();
    }

    public MalformedBgzipDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public MalformedBgzipDataException(String message) {
        super(message);
    }

    public MalformedBgzipDataException(Throwable cause) {
        super(cause);
    }

}
