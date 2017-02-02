import com.thinkaurelius.titan.core.TitanFactory;

g = TitanFactory.open('/Users/vikram/Code/titan/conf/titan-cassandra.properties');

g.traversal().E().drop().iterate();
g.traversal().V().drop().iterate();

System.exit(0);
