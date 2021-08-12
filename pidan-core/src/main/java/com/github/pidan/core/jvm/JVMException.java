package com.github.pidan.core.jvm;

public class JVMException
        extends RuntimeException
{
    private static final long serialVersionUID = -1L;

    public JVMException(String message)
    {
        super(message);
    }

    public JVMException(Throwable cause)
    {
        super(cause);
    }

    public JVMException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
