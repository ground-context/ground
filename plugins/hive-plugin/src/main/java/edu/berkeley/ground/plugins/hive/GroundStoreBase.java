/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.plugins.hive;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

public abstract class GroundStoreBase implements RawStore, Configurable {
  @Override
  public boolean alterDatabase(String dbname, Database db)
      throws NoSuchObjectException, MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    return new ArrayList<>();
  }

  @Override
  public boolean createType(Type type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Type getType(String typeName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropType(String typeName) {
    throw new UnsupportedOperationException();
  }

  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean addPartitions(String dbName,
                               String tblName,
                               PartitionSpecProxy partitionSpec,
                               boolean ifNotExists)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    // TODO Auto-generated method stub
    return false;
  }

  public void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  public List<String> getTables(String dbName, String pattern) throws MetaException {
    return getAllTables(dbName); // fix regex
  }

  public List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws MetaException, UnknownDBException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> listPartitionNames(String dbName, String tblName, short maxParts)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> listPartitionNamesByFilter(String dbName,
                                                 String tblName,
                                                 String filter,
                                                 short maxParts)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public void alterPartition(String dbName,
                             String tblName,
                             List<String> partVals,
                             Partition newPart)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
  }

  public void alterPartitions(String dbName,
                              String tblName,
                              List<List<String>> partValsList,
                              List<Partition> newParts)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  public boolean addIndex(Index index) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  public Index getIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean dropIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  public List<Index> getIndexes(String dbName, String origTableName, int max)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> listIndexNames(String dbName, String origTableName, short max)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  public List<Partition> getPartitionsByFilter(String dbName,
                                               String tblName,
                                               String filter,
                                               short maxParts)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean getPartitionsByExpr(String dbName,
                                     String tblName,
                                     byte[] expr,
                                     String defaultPartitionName,
                                     short maxParts,
                                     List<Partition> result) throws TException {
    // TODO Auto-generated method stub
    return false;
  }

  public int getNumPartitionsByFilter(String dbName, String tblName, String filter)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return 0;
  }

  public List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public Table markPartitionForEvent(String dbName,
                                     String tblName,
                                     Map<String, String> partVals,
                                     PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
      UnknownPartitionException {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean isPartitionMarkedForEvent(String dbName,
                                           String tblName,
                                           Map<String, String> partName,
                                           PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
      UnknownPartitionException {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
                           PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean revokeRole(Role role,
                            String userName,
                            PrincipalType principalType,
                            boolean grantOption)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName,
                                                 String userName,
                                                 List<String> groupNames)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName,
                                                    String tableName,
                                                    String userName,
                                                    List<String> groupNames)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName,
                                                        String tableName,
                                                        String partition,
                                                        String userName,
                                                        List<String> groupNames)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName,
                                                     String tableName,
                                                     String partitionName,
                                                     String columnName,
                                                     String userName,
                                                     List<String> groupNames)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
                                                             PrincipalType principalType) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
                                                         PrincipalType principalType,
                                                         String dbName) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
                                                      PrincipalType principalType,
                                                      String dbName,
                                                      String tableName) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
                                                                PrincipalType principalType,
                                                                String dbName,
                                                                String tableName,
                                                                List<String> partValues,
                                                                String partName) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
                                                                  PrincipalType principalType,
                                                                  String dbName,
                                                                  String tableName,
                                                                  String columnName) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
                                                                      PrincipalType principalType,
                                                                      String dbName,
                                                                      String tableName,
                                                                      List<String> partValues,
                                                                      String partName,
                                                                      String columnName) {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean grantPrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  public Role getRole(String roleName) throws NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> listRoleNames() {
    return new ArrayList<>();
  }

  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    // TODO Auto-generated method stub
    return null;
  }

  public Partition getPartitionWithAuth(String dbName,
                                        String tblName,
                                        List<String> partVals,
                                        String userName,
                                        List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<Partition> getPartitionsWithAuth(String dbName,
                                               String tblName,
                                               short maxParts,
                                               String userName,
                                               List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> listPartitionNamesPs(String dbName,
                                           String tblName,
                                           List<String> partVals,
                                           short maxParts)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<Partition> listPartitionsPsWithAuth(String dbName,
                                                  String tblName,
                                                  List<String> partVals,
                                                  short maxParts,
                                                  String userName,
                                                  List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    // TODO Auto-generated method stub
    return false;
  }

  public ColumnStatistics getTableColumnStatistics(String dbName,
                                                   String tableName,
                                                   List<String> colName)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName,
                                                             String tblName,
                                                             List<String> partNames,
                                                             List<String> colNames)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean deletePartitionColumnStatistics(String dbName,
                                                 String tableName,
                                                 String partName,
                                                 List<String> partVals,
                                                 String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    // TODO Auto-generated method stub
    return false;
  }

  public long cleanupEvents() {
    // TODO Auto-generated method stub
    return 0;
  }

  public boolean addToken(String tokenIdentifier, String delegationToken) {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean removeToken(String tokenIdentifier) {
    // TODO Auto-generated method stub
    return false;
  }

  public String getToken(String tokenIdentifier) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> getAllTokenIdentifiers() {
    // TODO Auto-generated method stub
    return null;
  }

  public int addMasterKey(String key) throws MetaException {
    // TODO Auto-generated method stub
    return 0;
  }

  public void updateMasterKey(Integer seqNo, String key)
      throws NoSuchObjectException, MetaException {
    // TODO Auto-generated method stub
  }

  public boolean removeMasterKey(Integer keySeq) {
    // TODO Auto-generated method stub
    return false;
  }

  public String[] getMasterKeys() {
    // TODO Auto-generated method stub
    return null;
  }

  public void verifySchema() throws MetaException {
    // TODO Auto-generated method stub

  }

  public String getMetaStoreSchemaVersion() throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
    // TODO Auto-generated method stub
  }

  public void dropPartitions(String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub

  }

  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName,
                                                            PrincipalType principalType) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
                                                               PrincipalType principalType) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
                                                                   PrincipalType principalType) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
                                                                     PrincipalType principalType) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName,
      PrincipalType principalType) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName,
                                                                String tableName,
                                                                String partitionName,
                                                                String columnName) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName,
                                                          String tableName,
                                                          String partitionName) {
    // TODO Auto-generated method stub
    return null;
  }

  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName,
                                                            String tableName,
                                                            String columnName) {
    // TODO Auto-generated method stub
    return null;
  }

  public void createFunction(Function func) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
  }

  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
  }

  public void dropFunction(String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    // TODO Auto-generated method stub
  }

  public Function getFunction(String dbName, String funcName) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<Function> getAllFunctions() throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> getFunctions(String dbName, String pattern) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public AggrStats get_aggr_stats_for(String dbName,
                                      String tblName,
                                      List<String> partNames,
                                      List<String> colNames)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    // TODO Auto-generated method stub
    return null;
  }

  public void addNotificationEvent(NotificationEvent event) {
    // TODO Auto-generated method stub
  }

  public void cleanNotificationEvents(int olderThan) {
    // TODO Auto-generated method stub
  }

  public CurrentNotificationEventId getCurrentNotificationEventId() {
    // TODO Auto-generated method stub
    return null;
  }

  public void flushCache() {
    // TODO Auto-generated method stub
  }

  public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public void putFileMetadata(List<Long> fileIds,
                              List<ByteBuffer> metadata,
                              FileMetadataExprType type)
      throws MetaException {
    // TODO Auto-generated method stub
  }

  public boolean isFileMetadataSupported() {
    // TODO Auto-generated method stub
    return false;
  }

  public void getFileMetadataByExpr(List<Long> fileIds,
                                    FileMetadataExprType type,
                                    byte[] expr,
                                    ByteBuffer[] metadata,
                                    ByteBuffer[] exprResults,
                                    boolean[] eliminated) throws MetaException {
    // TODO Auto-generated method stub
  }

  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    // TODO Auto-generated method stub
    return null;
  }

  public int getTableCount() throws MetaException {
    // TODO Auto-generated method stub
    return 0;
  }

  public int getPartitionCount() throws MetaException {
    // TODO Auto-generated method stub
    return 0;
  }

  public int getDatabaseCount() throws MetaException {
    // TODO Auto-generated method stub
    return 0;
  }

  public List<SQLPrimaryKey> getPrimaryKeys(String dbName, String tblName) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<SQLForeignKey> getForeignKeys(String parentDbName,
                                            String parentTblName,
                                            String foreignDbName,
                                            String foreignTblName) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  public void createTableWithConstraints(Table tbl,
                                         List<SQLPrimaryKey> primaryKeys,
                                         List<SQLForeignKey> foreignKeys)
      throws InvalidObjectException, MetaException {
    // TODO (FIX)
    createTable(tbl);
  }

  public void dropConstraint(String dbName, String tableName,
                             String constraintName) throws NoSuchObjectException {
    // TODO Auto-generated method stub

  }

  public void addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

}
