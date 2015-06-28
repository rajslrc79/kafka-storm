package kafka.storm.hdfs;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class MyTopology {

	public static void main (String args[]) throws AlreadyAliveException, InvalidTopologyException
	{
		// Kafka Config
		SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts("sandbox.hortonworks.com:2181"),"test","/kafka","KafkaSpout");
		
		
		// HDFS Bolt Config
			// Use pipe as record boundary
			RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");
	
			//Synchronize data buffer with the filesystem every 1000 tuples
			SyncPolicy syncPolicy = new CountSyncPolicy(1000);
	
			// Rotate data files when they reach five MB
			FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
	
			// Use default, Storm-generated file names
			FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/kafka");
	
			HdfsBolt bolt = new HdfsBolt()
	        .withFsUrl("hdfs://sandbox.hortonworks.com:8020")
	        .withFileNameFormat(fileNameFormat)
	        .withRecordFormat(format)
	        .withRotationPolicy(rotationPolicy)
	        .withSyncPolicy(syncPolicy);
		
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka_spout", new KafkaSpout(kafkaConfig));
		builder.setBolt("simplebolt",new SimpleBolt()).shuffleGrouping("kafka_spout");
		builder.setBolt("hdfs_bolt", bolt).shuffleGrouping("simplebolt");
		
		Config conf = new Config();
		
		conf.setDebug(false);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(1000000);
		cluster.killTopology("test");
		cluster.shutdown();
	
	}
}