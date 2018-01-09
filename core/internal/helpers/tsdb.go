package helpers

import (
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

type TSDBSender struct {
	AppContext  *protocol.ApplicationContext
	tsdbClient  client.Client
	measurement string
	database    string
	Log         *zap.Logger
}

func (sender *TSDBSender) Config(configRoot string) {
	addr := viper.GetString(configRoot + ".tsdbAddr")
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: addr,
	})
	if err != nil {
		sender.Log.Error("tsdb config error", zap.String("tsdb error", err.Error()))
		return
	}
	sender.measurement = viper.GetString(configRoot + ".measurement")
	sender.database = viper.GetString(configRoot + ".database")
	sender.tsdbClient = c
}

func (sender *TSDBSender) SendTransformLags(statusList []*protocol.ConsumerGroupStatus) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  sender.database,
		Precision: "s",
	})
	if err != nil {
		sender.Log.Error("tsdb error", zap.String("tsdb error", err.Error()))
		return
	}

	for _, consumerGroupStatus := range statusList {
		if consumerGroupStatus.Maxlag != nil {
			appId := strings.Split(consumerGroupStatus.Group, "-")[0]
			tags := map[string]string{
				"appId": 		 appId,
				"cluster":       consumerGroupStatus.Cluster,
				"consumerGroup": consumerGroupStatus.Group,
				"topic":         consumerGroupStatus.Maxlag.Topic,
				"complete":      strconv.FormatFloat(float64(consumerGroupStatus.Complete), 'E', -1, 32),

			}
			fields := map[string]interface{}{
				"partitionCount": consumerGroupStatus.TotalPartitions,
				"totalLag":       int(consumerGroupStatus.TotalLag),
				"status":         consumerGroupStatus.Status,
			}
			pt, err := client.NewPoint(sender.measurement, tags, fields, time.Now())
			if err != nil {
				sender.Log.Error("error create point", zap.String("tsdb error", err.Error()))
				return
			}
			bp.AddPoint(pt)
		}
	}

	// Write the batch
	if err := sender.tsdbClient.Write(bp); err != nil {
		sender.Log.Error("error send point", zap.String("tsdb error", err.Error()))
		return
	}
}
