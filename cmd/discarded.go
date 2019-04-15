// Copyright © 2018 Grafana Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"time"

	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var flags struct {
	invalidTimestamp bool
	invalidInterval  bool
	invalidOrgID     bool
	invalidName      bool
	invalidMtype     bool
	invalidTags      bool
	outOfOrder       bool
	duplicate        bool
}

var discardedCmd = &cobra.Command{
	Use:   "discarded",
	Short: "Sends out invalid/out-of-order/duplicate metric that will be discarded",
	Run: func(cmd *cobra.Command, args []string) {
		initStats(true, "discarded")
		outs := getOutputs()
		if len(outs) == 0 {
			log.Fatal("need to define an output")
		}

		generateData(outs)
	},
}

func init() {
	rootCmd.AddCommand(discardedCmd)
	discardedCmd.PersistentFlags().BoolVar(&flags.invalidTimestamp, "invalid-timestamp", false, "use an invalid timestamp")
	discardedCmd.PersistentFlags().BoolVar(&flags.invalidInterval, "invalid-interval", false, "use an invalid interval")
	discardedCmd.PersistentFlags().BoolVar(&flags.invalidOrgID, "invalid-orgid", false, "use an invalid orgId")
	discardedCmd.PersistentFlags().BoolVar(&flags.invalidName, "invalid-name", false, "use an invalid name")
	discardedCmd.PersistentFlags().BoolVar(&flags.invalidMtype, "invalid-mtype", false, "use an invalid mtype")
	discardedCmd.PersistentFlags().BoolVar(&flags.invalidTags, "invalid-tags", false, "use an invalid tag")
	discardedCmd.PersistentFlags().BoolVar(&flags.outOfOrder, "out-of-order", false, "send data in the wrong order")
	discardedCmd.PersistentFlags().BoolVar(&flags.duplicate, "duplicate", false, "send duplicate data")
}

func generateData(outs []out.Out) {
	md := &schema.MetricData{
		Name:     "some.id.of.a.metric.0",
		OrgId:    1,
		Interval: 1,
		Unit:     "s",
		Mtype:    "gauge",
		Tags:     nil,
	}

	if flags.invalidInterval {
		md.Interval = 0 // 0 or >= math.MaxInt32
	}

	if flags.invalidOrgID {
		md.OrgId = 0
	}

	if flags.invalidName {
		md.Name = ""
	}

	if flags.invalidMtype {
		md.Mtype = "invalid Mtype"
	}

	if flags.invalidTags {
		md.Tags = []string{"==invalid tags,#4561=="}
	}

	md.SetId()
	sl := []*schema.MetricData{md}

	tick := time.NewTicker(time.Second)
	for ts := range tick.C {
		timestamp := ts.Unix()
		if flags.invalidTimestamp {
			timestamp = 0 // 0 or >= math.MaxInt32
		} else if flags.outOfOrder {
			n := int64(3)
			// every n seconds, emit data points in the past for n seconds
			if timestamp%(2*n) < n {
				timestamp -= n
			}
		} else if flags.duplicate {
			if md.Time != 0 {
				timestamp = md.Time
			}
		}
		md.Time = timestamp
		md.Value = float64(2.0)
		fmt.Printf("Sending MetricData: %+v\n", md)
		for _, o := range outs {
			o.Flush(sl)
		}
	}
}
