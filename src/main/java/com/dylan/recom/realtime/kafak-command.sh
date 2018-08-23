以下是kafka常用命令行总结：

0.查看有哪些主题：
kafka-topics --list --zookeeper 127.0.0.1:2181

1.查看topic的详细信息
kafka-topics -zookeeper 127.0.0.1:2181 -describe -topic recom

2、为topic增加副本
kafka-reassign-partitions -zookeeper 127.0.0.1:2181 -reassignment-json-file json/partitions-to-move.json -execute

3、创建topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic recom

4、为topic增加partition
kafka-topics –zookeeper 127.0.0.1:2181 –alter –partitions 20 –topic recom

5、kafka生产者
//kafka-console-producer --broker-list localhost:9092 --topic recom
//kafka-console-producer --broker-list 127.0.0.1:9092 --topic recom
kafka-console-producer --broker-list 192.168.30.215:9092 --topic recom


kafka-console-producer --bootstrap-servers 192.168.30.215:9092 --topic recom

6、kafka消费者
旧api
kafka-console-consumer --zookeeper localhost:2181 --from-beginning --topic recom --group group1
kafka-console-consumer --zookeeper localhost:2181 --topic recom --group group1
新api
kafka-console-consumer --bootstrap-server 192.168.30.215:9092 --topic recom --group group2


7、kafka服务启动
kafka-server-start -daemon ../config/server.properties

8、下线broker
kafka-run-class kafka.admin.ShutdownBroker --zookeeper 127.0.0.1:2181 --broker #brokerId# --num.retries 3 --retry.interval.ms 60
shutdown broker

9、删除topic
kafka-run-class kafka.admin.DeleteTopicCommand --topic recom --zookeeper 127.0.0.1:2181
kafka-topics --zookeeper localhost:2181 --delete --topic recom

10、查看consumer组内消费的offset console-consumer-52592
kafka-run-class kafka.tools.ConsumerOffsetChecker --zookeeper localhost:2181 --group group1 --topic recom
kafka-consumer-offset-checker --zookeeper 192.168.0.201:12181 --group group1 --topic group1

kafka-consumer-groups  --bootstrap-server 127.0.0.1:9092 --group group1 --describe