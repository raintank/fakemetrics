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
	"log"
	"time"

	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/schema"
	"github.com/spf13/cobra"
)

var oooCmd = &cobra.Command{
	Use:   "ooo",
	Short: "Send out-of-order metric data",
	Run: func(cmd *cobra.Command, args []string) {
		initStats(true, "ooo")
		outs := getOutputs()
		if len(outs) == 0 {
			log.Fatal("need to define an output")
		}
		ooo(outs)
	},
}

func init() {
	rootCmd.AddCommand(oooCmd)
}

// clock - send ts
// 5         10
// 6         11
// 7         12
// 8         13
// 9         14
// 10     5
// 11     6
// 12     7
// 13     8
// 14     9
// 15        20
// 16        21
// 17        22
// 18        23
// 19        24
// 20    15
func ooo(outs []out.Out) {
	md := &schema.MetricData{
		Name:     "fakemetrics.ooo",
		OrgId:    1,
		Interval: 1,
		Unit:     "s",
		Mtype:    "gauge",
		Tags:     []string{"data=out-of-order"},
	}

	md.SetId()
	sl := []*schema.MetricData{md}
	tick := time.NewTicker(time.Second)
	for ts := range tick.C {
		unix := ts.Unix()
		if unix%10 < 5 {
			md.Time = unix - 5
		} else {
			md.Time = unix + 5
		}
		md.Value = float64(md.Time)
		for _, o := range outs {
			o.Flush(sl)
		}
	}
}
