---
title: "kafka消费者组重分配协议及分配策略"
date: 2022-09-02T16:38:08+08:00
draft: true

tags: ["kafka"]
categories: ["技术"]
---




##

```java
// 将consumer订阅的topics做一次转换，转换为map，key是consumerID，value是该consumer订阅的topic列表
private Map<String, List<String>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {  
    Map<String, List<String>> res = new HashMap<>();  
    for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {  
        String consumerId = subscriptionEntry.getKey();  
        for (String topic : subscriptionEntry.getValue().topics())  
            put(res, topic, consumerId);  
    }  
    return res;  
}
```

```java
// 执行分区分配，返回map，key是consumerID，value是其分配到的topicPartition列表
@Override  
public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,  
                                                Map<String, Subscription> subscriptions) {  
    Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);  
    Map<String, List<TopicPartition>> assignment = new HashMap<>();  
    for (String memberId : subscriptions.keySet())  
        assignment.put(memberId, new ArrayList<>());  
  
    for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {  
        String topic = topicEntry.getKey();  
        List<String> consumersForTopic = topicEntry.getValue();  
  
        Integer numPartitionsForTopic = partitionsPerTopic.get(topic);  
        if (numPartitionsForTopic == null)  
            continue;  
  
        Collections.sort(consumersForTopic);  
  
        int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();  
        int consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();  
  
        List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);  
        for (int i = 0, n = consumersForTopic.size(); i < n; i++) {  
            int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);  
            int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);  
            assignment.get(consumersForTopic.get(i)).addAll(partitions.subList(start, start + length));  
        }  
    }    return assignment;  
}
```
