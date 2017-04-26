import org.junit.*;

import play.libs.F.*;
import play.mvc.*;
import play.test.*;
import play.twirl.api.Content;

import static org.junit.Assert.*;
import static play.test.Helpers.*;

/**
 * Simple (JUnit) tests that can call all parts of a play app. If you are interested in mocking a
 * whole application, see the wiki for more details.
 */
public class ApplicationTest {

  @Test
  public void simpleCheck() {
    int a = 1 + 1;
    assertEquals(2, a);
  }

  @Test
  public void renderTemplate() {
    Content html = views.html.index.render("Your new application is ready.");
    assertEquals("text/html", html.contentType());
    assertTrue(html.body().contains("Your new application is ready."));
  }
}
