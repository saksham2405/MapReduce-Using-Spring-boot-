package com.saksham.mapreduce;

public class NoTaskRecievedException extends Exception{

    private String message;

    public NoTaskRecievedException(String message) {
        this.message = message;
    }
}
