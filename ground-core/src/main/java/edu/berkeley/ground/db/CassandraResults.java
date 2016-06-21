package edu.berkeley.ground.db;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CassandraResults implements QueryResults {
    private ResultSet resultSet;
    private Row currentRow;

    public CassandraResults(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.currentRow = resultSet.one();
    }

    public String getString(int index) throws GroundException {
        try {
            return this.currentRow.getString(index);
        } catch (Exception e) {
            throw new GroundException(e);
        }
    }

    public int getInt(int index) throws GroundException {
        try {
            return this.currentRow.getInt(index);
        } catch (Exception e) {
            throw new GroundException(e);
        }
    }

    public boolean getBoolean(int index) throws GroundException {
        try {
            return this.currentRow.getBool(index);
        } catch (Exception e) {
            throw new GroundException(e);
        }
    }

    public List<String> getStringList(int index) throws GroundException {
        try {
            Iterator<Row> rowIterator = this.resultSet.iterator();
            List<String> result = new ArrayList<>();

            while(rowIterator.hasNext()) {
                result.add(rowIterator.next().getString(index));
            }

            return result;
        } catch (Exception e) {
            throw new GroundException(e);
        }
    }

    public boolean next() throws GroundException {
        this.currentRow = this.resultSet.one();

        return this.currentRow != null;
    }
}
