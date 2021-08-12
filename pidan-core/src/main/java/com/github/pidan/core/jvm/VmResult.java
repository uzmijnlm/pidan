package com.github.pidan.core.jvm;

import java.io.Serializable;

public class VmResult<V extends Serializable>
        implements Serializable
{
    private V result;
    private String errorMessage;
    private final boolean isFailed;

    public V get()
            throws JVMException
    {
        if (isFailed()) {
            throw new JVMException("ForKJVMError: " + errorMessage);
        }
        return result;
    }

    public boolean isFailed()
    {
        return isFailed;
    }

    public String getOnFailure()
    {
        return errorMessage;
    }

    public VmResult(Serializable result)
    {
        this.result = (V) result;
        this.isFailed = false;
    }

    public VmResult(String errorMessage)
    {
        this.errorMessage = errorMessage;
        this.isFailed = true;
    }
}
