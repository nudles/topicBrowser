package util;

public class UnimplementedException extends RuntimeException
{

    /**
     *throw this exception if some methods should be implemented but not 
     */
    private static final long serialVersionUID = 8219816186799352118L;

	public UnimplementedException(String msg)
	{
		super(msg);
	}
}
