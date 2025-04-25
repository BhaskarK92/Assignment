##########TASK1###########
#Create Topic KeyLessTopic
cd $KAFKA_HOME
./bin/kafka-topics.sh --create --topic KeyLessTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1

#Running a console producer
cd $KAFKA_HOME
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic KeyLessTopic

##########TASK2###########
#Running a console consumer
cd $KAFKA_HOME
./bin/kafka-console-consumer.sh --topic KeyLessTopic --from-beginning --zookeeper localhost:2181


##########TASK3###########
#Create Topic KeyedTopic
cd $KAFKA_HOME
./bin/kafka-topics.sh --create --topic KeyedTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1

#Running a console producer  with making Key True and Key Separator ","
cd $KAFKA_HOME
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic KeyedTopic \
--property "parse.key=true" \
--property "key.separator=,"

#Running a console consumer  by making key property true
cd $KAFKA_HOME
./bin/kafka-console-consumer.sh --topic KeyedTopic --from-beginning \
--zookeeper localhost:2181 \
--property print.key=true


##########TASK4###########
#Running a console producer  with making Key True and Key Separator "-"
cd $KAFKA_HOME
./bin/kafka-console-consumer.sh --topic KeyedTopic --from-beginning \
--zookeeper localhost:2181 \
--property print.key=true \
--property key.separator="-" \
--from-beginning