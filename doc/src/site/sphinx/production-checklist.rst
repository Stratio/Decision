Production Checklist & Trouble Shooting Guide
*********************************************
**Decision require that Zookeeper and Kafka are running**

When you start the engine you have to ensure that you have Zookeeper and Kafka instances running and correctly
configured in your config file located in /etc/sds/decision/conf.

**If shell show a message that said that in unable connect with Decision.**

When you run Decision shell you have to ensure that you have Zookeeper and Kafka running and Decision
engine too. You must verify that Zookeeper and Kafka routes are properly configured in /opt/sds/decision-shell/shell
.properties.

**If you are working with failover mode and you need to delete all your queries.**

In that case, you have to run your ./zkCli.sh on your zookeeper node and in the zookeeper shell execute *rmr failover*.