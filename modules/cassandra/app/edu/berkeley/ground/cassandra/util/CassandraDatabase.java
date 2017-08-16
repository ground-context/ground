/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.cassandra.util;


import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;


public class CassandraDatabase {
//  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);

  private final String host;
  private final int port;
  private final String keyspace;
  private final String username;
  private final String password;

  private final Cluster cluster;
  private final Session session;

  /**
   * Constructor for the Cassandra client.
   *
   * @param host the host address for Cassandra
   * @param port the Cassandra port
   * @param keyspace the name of the keyspace we're using
   * @param username the login username
   * @param password the login password
   */
  public CassandraDatabase(String host, int port, String keyspace, String username, String password) {
    this.host = host;
    this.port = port;
    this.keyspace = keyspace;
    this.username = username;
    this.password = password;

    this.cluster = this.makeCluster();
    this.session = this.makeSession();
  }

  /**
   * Returns the host of this Cassandra Database
   * @return host
   */
  public String getHost() {
    return this.host;
  }

  /**
   * Returns the port of this Cassandra Database
   * @return port
   */
  public int getPort() {
    return this.port;
  }

  /**
   * Returns the keyspace of this Cassandra Database
   * @return keyspace
   */
  public String getKeyspace() {
    return this.keyspace;
  }

  /**
   * Returns the username of this Cassandra Database
   * @return username
   */
  public String getUsername() {
    return this.username;
  }

  /**
   * Returns the password of this Cassandra Database
   * @return password
   */
  public String getPassword() {
    return this.password;
  }

  /**
   * Returns a new Session for this Cassandra Database
   * @return session
   */
  private Cluster makeCluster() {
    return Cluster.builder()
      .addContactPoint(this.host)
      .withPort(this.port)
      .withAuthProvider(new PlainTextAuthProvider(this.username, this.password))
      .build();
  }

  /**
   * Returns a new Session for a given cluster
   * @param cluster
   * @return session
   */
  private Session makeSession() {
    return this.cluster.connect(this.keyspace);
  }

  public Session getSession() {
    return this.session;
  }

  public void shutdown() {
    this.session.close();
    this.cluster.close();
  }
}
