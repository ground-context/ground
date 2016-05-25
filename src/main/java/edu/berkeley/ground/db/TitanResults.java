package edu.berkeley.ground.db;

import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;

public class TitanResults implements QueryResults {
    public String getString(int index) throws GroundException {
        return null;
    }

    public int getInt(int index) throws GroundException {
        return 0;
    }

    public boolean getBoolean(int index) throws GroundException {
        return false;
    }

    public List<String> getStringList(int index) throws GroundException {
        return null;
    }

    public boolean next() throws GroundException {
        return false;
    }
}
