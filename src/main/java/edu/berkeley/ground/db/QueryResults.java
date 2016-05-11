package edu.berkeley.ground.db;

import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;

public interface QueryResults {
    String getString(int index) throws GroundException;

    int getInt(int index) throws GroundException;

    boolean getBoolean(int index) throws GroundException;

    List<String> getStringList() throws GroundException;

    boolean next() throws GroundException;
}