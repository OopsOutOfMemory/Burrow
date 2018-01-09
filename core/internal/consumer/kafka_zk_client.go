/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package consumer

import (
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
	"encoding/json"
)

type topicList struct {
	topics map[string]*partitionCount
	lock   *sync.Mutex
}
type partitionCount struct {
	count int32
	lock  *sync.Mutex
}

//change by shengli
type Transform struct {
	Topic string `json:"topic"`
	PartitionId int32 `json:"partitionId"`
	Offset int64 `json:"offset"`
}

// KafkaZkClient is a consumer module which connects to the Zookeeper ensemble where an Apache Kafka cluster maintains
// metadata, and reads consumer group information from the /consumers tree (older ZK-based consumers). It uses watches
// to monitor every group and offset, and the information is forwarded to the storage subsystem for use in evaluations.
type KafkaZkClient struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name             string
	cluster          string
	servers          []string
	zookeeperTimeout int
	zookeeperPath    string

	zk             protocol.ZookeeperClient
	areWatchesSet  bool
	running        *sync.WaitGroup
	groupLock      *sync.Mutex
	groupList      map[string]*topicList
	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp
	connectFunc    func([]string, time.Duration, *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error)
}

// Configure validates the configuration for the consumer. At minimum, there must be a cluster name to which these
// consumers belong, as well as a list of servers provided for the Zookeeper ensemble, of the form host:port. If not
// explicitly configured, it is assumed that the Kafka cluster metadata is present in the ensemble root path. If the
// cluster name is unknown, or if the server list is missing or invalid, this func will panic.
func (module *KafkaZkClient) Configure(name string, configRoot string) {
	module.Log.Info("configuring")

	module.name = name
	module.running = &sync.WaitGroup{}
	module.groupLock = &sync.Mutex{}
	module.groupList = make(map[string]*topicList)
	module.connectFunc = helpers.ZookeeperConnect

	module.servers = viper.GetStringSlice(configRoot + ".servers")
	if len(module.servers) == 0 {
		panic("No Zookeeper servers specified for consumer " + module.name)
	} else if !helpers.ValidateHostList(module.servers) {
		panic("Consumer '" + name + "' has one or more improperly formatted servers (must be host:port)")
	}

	// Set defaults for configs if needed, and get them
	viper.SetDefault(configRoot+".zookeeper-timeout", 30)
	module.zookeeperTimeout = viper.GetInt(configRoot + ".zookeeper-timeout")
	module.zookeeperPath = viper.GetString(configRoot+".zookeeper-path") + "/consumers"
	module.cluster = viper.GetString(configRoot + ".cluster")

	if !helpers.ValidateZookeeperPath(module.zookeeperPath) {
		panic("Consumer '" + name + "' has a bad zookeeper path configuration")
	}

	whitelist := viper.GetString(configRoot + ".group-whitelist")
	if whitelist != "" {
		re, err := regexp.Compile(whitelist)
		if err != nil {
			module.Log.Panic("Failed to compile group whitelist")
			panic(err)
		}
		module.groupWhitelist = re
	}

	blacklist := viper.GetString(configRoot + ".group-blacklist")
	if blacklist != "" {
		re, err := regexp.Compile(blacklist)
		if err != nil {
			module.Log.Panic("Failed to compile group blacklist")
			panic(err)
		}
		module.groupBlacklist = re
	}
}

// Start connects to the Zookeeper ensemble configured. Any error connecting to the cluster is returned to the caller.
// Once the client is set up, the consumer group list is enumerated and watches are set up for each group, topic,
// partition, and offset. A goroutine is also started to monitor the Zookeeper connection state, and reset the watches
// in the case the the session expires.
func (module *KafkaZkClient) Start() error {
	module.Log.Info("starting")

	zkconn, connEventChan, err := module.connectFunc(module.servers, time.Duration(module.zookeeperTimeout)*time.Second, module.Log)
	if err != nil {
		return err
	}
	module.zk = zkconn

	// Set up all groups initially (we can't count on catching the first CONNECTED event
	module.running.Add(1)
	module.resetGroupListWatchAndAdd(false)
	module.areWatchesSet = true

	// Start up a func to watch for connection state changes and reset all the watches when needed
	module.running.Add(1)
	go module.connectionStateWatcher(connEventChan)

	return nil
}

// Stop closes the Zookeeper client.
func (module *KafkaZkClient) Stop() error {
	module.Log.Info("stopping")

	// Closing the ZK client will invalidate all the watches, which will close all the running goroutines
	module.zk.Close()
	module.running.Wait()

	return nil
}

func (module *KafkaZkClient) connectionStateWatcher(eventChan <-chan zk.Event) {
	defer module.running.Done()
	for event := range eventChan {
		if event.Type == zk.EventSession {
			switch event.State {
			case zk.StateExpired:
				module.Log.Error("session expired")
				module.areWatchesSet = false
			case zk.StateConnected:
				if !module.areWatchesSet {
					module.Log.Info("reinitializing watches")
					module.groupLock.Lock()
					module.groupList = make(map[string]*topicList)
					module.groupLock.Unlock()

					module.running.Add(1)
					go module.resetGroupListWatchAndAdd(false)
				}
			}
		}
	}
}

func (module *KafkaZkClient) acceptConsumerGroup(group string) bool {
	// No whitelist means everything passes
	if (module.groupWhitelist != nil) && (!module.groupWhitelist.MatchString(group)) {
		return false
	}
	if (module.groupBlacklist != nil) && module.groupBlacklist.MatchString(group) {
		return false
	}
	return true
}

func (module *KafkaZkClient) watchGroupList(eventChan <-chan zk.Event) {
	defer module.running.Done()

	event, isOpen := <-eventChan
	if (!isOpen) || (event.Type == zk.EventNotWatching) {
		// We're done here
		return
	}
	module.running.Add(1)
	go module.resetGroupListWatchAndAdd(event.Type != zk.EventNodeChildrenChanged)
}

func (module *KafkaZkClient) resetGroupListWatchAndAdd(resetOnly bool) {
	defer module.running.Done()

	// Get the current group list and reset our watch
	consumerGroups, _, groupListEventChan, err := module.zk.ChildrenW(module.zookeeperPath)
	if err != nil {
		// Can't read the consumers path. Bail for now
		module.Log.Error("failed to list groups", zap.String("error", err.Error()))
		return
	}
	module.running.Add(1)
	go module.watchGroupList(groupListEventChan)

	if !resetOnly {
		// Check for any new groups and create the watches for them
		module.groupLock.Lock()
		defer module.groupLock.Unlock()
		for _, group := range consumerGroups {
			if !module.acceptConsumerGroup(group) {
				module.Log.Debug("skip group",
					zap.String("group", group),
					zap.String("reason", "whitelist"),
				)
				continue
			}

			if module.groupList[group] == nil {
				module.groupList[group] = &topicList{
					topics: make(map[string]*partitionCount),
					lock:   &sync.Mutex{},
				}
				module.Log.Debug("add group",
					zap.String("group", group),
				)
				module.running.Add(1)
				module.resetTopicListWatchAndAdd(group, false)
			}
		}
	}
}

func (module *KafkaZkClient) watchTopicList(group string, eventChan <-chan zk.Event) {
	defer module.running.Done()

	event, isOpen := <-eventChan
	if (!isOpen) || (event.Type == zk.EventNotWatching) {
		// We're done here
		return
	}
	module.running.Add(1)
	go module.resetTopicListWatchAndAdd(group, event.Type != zk.EventNodeChildrenChanged)
}

func (module *KafkaZkClient) resetTopicListWatchAndAdd(group string, resetOnly bool) {
	defer module.running.Done()
	// changed by shengli |Get the current group topic list and reset our watch|
	groupData, _, topicListEventChan, dataErr := module.zk.GetW(module.zookeeperPath + "/" + group + "/" + "0" )
	if dataErr != nil {
		module.Log.Debug("failed to get topic ",
			zap.String("group", group),
			zap.String("error", dataErr.Error()),
		)
		return
	}
	var transformConsumer Transform
	mErr := json.Unmarshal(groupData, &transformConsumer)
	if mErr != nil {
		module.Log.Debug("failed to marshal json of transform ",
			zap.String("group", group),
			zap.String("error", mErr.Error()),
		)
		return
	}
	// Get the current group topic list and reset our watch (transform group -> topic 1vs1)
	groupTopics := [1]string{transformConsumer.Topic}

	// End changed by shengli |Get the current group topic list and reset our watch|
	module.running.Add(1)
	go module.watchTopicList(group, topicListEventChan)

	if !resetOnly {
		// Check for any new topics and create the watches for them
		module.groupList[group].lock.Lock()
		defer module.groupList[group].lock.Unlock()
		for _, topic := range groupTopics {
			if module.groupList[group].topics[topic] == nil {
				module.groupList[group].topics[topic] = &partitionCount{
					count: 0,
					lock:  &sync.Mutex{},
				}
				module.Log.Debug("add topic",
					zap.String("group", group),
					zap.String("topic", topic),
				)
				module.running.Add(1)
				module.resetPartitionListWatchAndAdd(group, topic, false)
			}
		}
	}
}

func (module *KafkaZkClient) watchPartitionList(group string, topic string, eventChan <-chan zk.Event) {
	defer module.running.Done()

	event, isOpen := <-eventChan
	if (!isOpen) || (event.Type == zk.EventNotWatching) {
		// We're done here
		return
	}
	module.running.Add(1)
	go module.resetPartitionListWatchAndAdd(group, topic, event.Type != zk.EventNodeChildrenChanged)
}

func (module *KafkaZkClient) resetPartitionListWatchAndAdd(group string, topic string, resetOnly bool) {
	defer module.running.Done()
	// changed by shengli
	// Get the current topic partition list and reset our watch
	topicPartitions, _, partitionListEventChan, err := module.zk.ChildrenW(module.zookeeperPath + "/" + group )
	if err != nil {
		// Can't read the consumers path. Bail for now
		module.Log.Warn("failed to read partitions",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.String("error", err.Error()),
		)
		return
	}
	module.running.Add(1)
	go module.watchPartitionList(group, topic, partitionListEventChan)

	if !resetOnly {
		// Check for any new partitions and create the watches for them
		module.groupList[group].topics[topic].lock.Lock()
		defer module.groupList[group].topics[topic].lock.Unlock()
		if int32(len(topicPartitions)) >= module.groupList[group].topics[topic].count {
			for i := module.groupList[group].topics[topic].count; i < int32(len(topicPartitions)); i++ {
				module.Log.Debug("add partition",
					zap.String("group", group),
					zap.String("topic", topic),
					zap.Int32("partition", i),
				)
				module.running.Add(1)
				module.resetOffsetWatchAndSend(group, topic, i, false)
			}
			module.groupList[group].topics[topic].count = int32(len(topicPartitions))
		}
	}
}

func (module *KafkaZkClient) watchOffset(group string, topic string, partition int32, eventChan <-chan zk.Event) {
	defer module.running.Done()

	event, isOpen := <-eventChan
	if (!isOpen) || (event.Type == zk.EventNotWatching) {
		// We're done here
		return
	}
	module.running.Add(1)
	go module.resetOffsetWatchAndSend(group, topic, partition, event.Type != zk.EventNodeDataChanged)
}

func (module *KafkaZkClient) resetOffsetWatchAndSend(group string, topic string, partition int32, resetOnly bool) {
	defer module.running.Done()

	// Get the current offset and reset our watch
	offsetString, offsetStat, offsetEventChan, err := module.zk.GetW(module.zookeeperPath + "/" + group + "/" + strconv.FormatInt(int64(partition), 10))
	if err != nil {
		// Can't read the partition ofset path. Bail for now
		module.Log.Warn("failed to read offset",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.String("error", err.Error()),
		)
		return
	}
	var transformConsumer = Transform{}
	formatErr := json.Unmarshal(offsetString, &transformConsumer)
	if formatErr != nil {
		module.Log.Debug("failed to marshal offset json of transform ",
			zap.String("group", string(group)),
			zap.String("error", formatErr.Error()),
		)
		// Badly formatted offset
		module.Log.Error("badly formatted offset",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.ByteString("offset_string", offsetString),
			zap.String("error", err.Error()),
		)
		return
	}


	module.running.Add(1)
	go module.watchOffset(group, topic, partition, offsetEventChan)

	if !resetOnly {
		offset := transformConsumer.Offset

		// Send the offset to the storage module
		partitionOffset := &protocol.StorageRequest{
			RequestType: protocol.StorageSetConsumerOffset,
			Cluster:     module.cluster,
			Topic:       topic,
			Partition:   int32(partition),
			Group:       group,
			Timestamp:   offsetStat.Mtime,
			Offset:      offset,
		}
		module.Log.Debug("consumer offset",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Int64("offset", offset),
			zap.Int64("timestamp", offsetStat.Mtime),
		)
		helpers.TimeoutSendStorageRequest(module.App.StorageChannel, partitionOffset, 1)
	}
}
