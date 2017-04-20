package db;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.reflect.TypeToken;
import exceptions.GroundDbException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Mockito.*;

public class CassandraResultsTest {
  @Test
  public void shouldGetStringFromValidResultSet() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.getString("field")).thenReturn("value");
    when(rs.one()).thenReturn(row);
    CassandraResults results = new CassandraResults(rs);
    assertThat(results.getString("field")).isEqualTo("value");
  }

  @Test(expected = GroundDbException.class)
  public void shouldFailWhenFieldInvalid() throws GroundDbException {
    setupInvalidField("field").getString("field");
  }

  @Test(expected = GroundDbException.class)
  public void shouldFailWhenCodecNotFoundForString() throws GroundDbException {
    setupCodecNotFoundForField("field").getString("field");
  }

  @Test
  public void shouldGetBooleanFromValidResultSet() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.getString("trueField")).thenReturn("true");
    when(row.getString("falseField")).thenReturn("false");
    when(row.getString("trueCapitalizedField")).thenReturn("True");
    when(row.getString("falseCapitalField")).thenReturn("FALSE");
    when(row.getString("fooField")).thenReturn("foo");

    when(rs.one()).thenReturn(row);
    CassandraResults results = new CassandraResults(rs);
    assertThat(results.getBoolean("trueField")).isEqualTo(true);
    assertThat(results.getBoolean("falseField")).isEqualTo(false);
    assertThat(results.getBoolean("trueCapitalizedField")).isEqualTo(true);
    assertThat(results.getBoolean("falseCapitalField")).isEqualTo(false);
    assertThat(results.getBoolean("fooField")).isEqualTo(false);
  }

  @Test(expected = GroundDbException.class)
  public void shouldFailWhenCodecNotFoundForBoolean() throws GroundDbException {
    setupCodecNotFoundForField("field").getBoolean("field");
  }

  @Test(expected = GroundDbException.class)
  public void shouldFailWhenFieldInvalidForBoolean() throws GroundDbException {
    setupInvalidField("field").getBoolean("field");
  }

  @Test
  public void shouldGetIntegerFromValidResultSet() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.getString("field")).thenReturn("42");
    when(rs.one()).thenReturn(row);
    CassandraResults results = new CassandraResults(rs);
    assertThat(results.getInt("field")).isEqualTo(42);
  }

  @Test(expected = GroundDbException.class)
  public void shouldFailGetIntegerFromInvalidContent() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.getString("field")).thenReturn("not-int");
    when(rs.one()).thenReturn(row);
    new CassandraResults(rs).getInt("field");
  }

  @Test(expected = GroundDbException.class)
  public void shouldFailGetIntegerFromInvalidField() throws GroundDbException {
    setupInvalidField("field").getInt("field");
  }

  @Test(expected = GroundDbException.class)
  public void shouldFailWhenCodecNotFoundForInteger() throws GroundDbException {
    setupCodecNotFoundForField("field").getInt("field");
  }

  @Test
  public void shouldGetLongFromValidResultSet() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.getLong("field")).thenReturn(120L);
    when(rs.one()).thenReturn(row);
    CassandraResults results = new CassandraResults(rs);
    assertThat(results.getLong("field")).isEqualTo(120L);
  }

  @Test(expected = GroundDbException.class)
  public void shouldFailWhenCodecNotFoundForLong() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    CodecNotFoundException codecNotFound = new CodecNotFoundException("mock exception", DataType.blob(), TypeToken.of(Integer.class));
    when(row.getLong("field")).thenThrow(codecNotFound );
    when(rs.one()).thenReturn(row);
    new CassandraResults(rs).getLong("field");
  }

  @Test(expected = GroundDbException.class)
  public void shouldFailWhenIllegalArgumentForLong() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.getLong("field")).thenThrow(new IllegalArgumentException("invalid field"));
    when(rs.one()).thenReturn(row);
    new CassandraResults(rs).getLong("field");
  }

  @Test
  public void shouldMoveToNextRecord() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);
    when(row2.getString("field")).thenReturn("2");
    when(rs.one()).thenReturn(row1).thenReturn(row2);
    CassandraResults results = new CassandraResults(rs);
    assertThat(results.next()).isTrue();
    assertThat(results.getInt("field")).isEqualTo(2);
  }

  @Test
  public void shouldMoveUpToLastRecord() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);
    when(rs.one()).thenReturn(row1).thenReturn(row2).thenReturn(null);
    CassandraResults results = new CassandraResults(rs);
    assertThat(results.next()).isTrue();
    assertThat(results.next()).isFalse();
  }

  @Test
  public void shouldCheckANullField() throws GroundDbException {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.isNull("trueField")).thenReturn(true);
    when(row.isNull("falseField")).thenReturn(false);
    when(rs.one()).thenReturn(row);
    CassandraResults results = new CassandraResults(rs);
    assertThat(results.isNull("trueField")).isTrue();
    assertThat(results.isNull("falseField")).isFalse();
  }

  private CassandraResults setupInvalidField(String fieldName) {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.getString(fieldName)).thenThrow(new IllegalArgumentException("invalid field"));
    when(rs.one()).thenReturn(row);
    return new CassandraResults(rs);
  }

  private CassandraResults setupCodecNotFoundForField(String fieldName) {
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);
    CodecNotFoundException codecNotFound = new CodecNotFoundException("mock exception", DataType.blob(), TypeToken.of(Integer.class));
    when(row.getString(fieldName)).thenThrow(codecNotFound );
    when(rs.one()).thenReturn(row);
    return new CassandraResults(rs);
  }



}
