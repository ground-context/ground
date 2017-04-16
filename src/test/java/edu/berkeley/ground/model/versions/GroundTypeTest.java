package edu.berkeley.ground.model.versions;

import org.junit.Test;

import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class GroundTypeTest {

  @Test
  public void testGetTypeFromString() throws GroundException {
    assertEquals(GroundType.BOOLEAN, GroundType.fromString("boolean"));
    assertEquals(GroundType.INTEGER, GroundType.fromString("integer"));
    assertEquals(GroundType.LONG, GroundType.fromString("long"));
    assertEquals(GroundType.STRING, GroundType.fromString("string"));
  }

  @Test
  public void eachGroundTypeHasASqlType() {
    for (GroundType type:GroundType.values()){
      assertThat(type.getSqlType()).isNotEqualTo(java.sql.Types.NULL);
    }
  }

  @Test(expected = GroundException.class)
  public void testGetBadType() throws GroundException {
    GroundType.fromString("notType");
  }

  @Test
  public void testParse() throws GroundException {
    assertThat(GroundType.STRING.parse(null)).isNull();
    assertThat(GroundType.STRING.parse("hello")).isEqualTo("hello");
    assertThat(GroundType.INTEGER.parse("1234")).isEqualTo(1234);
    assertThat(GroundType.LONG.parse("54321")).isEqualTo(54321L);
    assertThat(GroundType.BOOLEAN.parse("true")).isEqualTo(true);
    assertThat(GroundType.BOOLEAN.parse("false")).isEqualTo(false);
  }
}
