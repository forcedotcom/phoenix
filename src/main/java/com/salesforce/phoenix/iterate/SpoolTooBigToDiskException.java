package com.salesforce.phoenix.iterate;

/**
 * Thrown by {@link com.salesforce.phoenix.iterate.SpoolingResultIterator } when
 * result is too big to fit into memory and too big to spool to disk.
 * 
 * @author haitaoyao
 * 
 */
public class SpoolTooBigToDiskException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SpoolTooBigToDiskException(String msg) {
		super(msg);
	}
}
