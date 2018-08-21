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
	"log"
	"time"

	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/schema"
	"github.com/spf13/cobra"
)

var badCmd = &cobra.Command{
	Use:   "bad",
	Short: "Send bad metric data",
	Run: func(cmd *cobra.Command, args []string) {
		initStats(true, "bad")
		outs := getOutputs()
		if len(outs) == 0 {
			log.Fatal("need to define an output")
		}
		bad(outs)
	},
}

func init() {
	rootCmd.AddCommand(badCmd)
}

func bad(outs []out.Out) {
	md := &schema.MetricData{
		Name:     ".foo.bar",
		OrgId:    1,
		Interval: 1,
		Unit:     "s",
		Mtype:    "gauge",
		Tags:     nil,
	}
	md.SetId()
	sl := []*schema.MetricData{md}
	tick := time.NewTicker(time.Second)
	for ts := range tick.C {
		fmt.Println("doing", ts)
		md.Time = ts.Unix()
		md.Value = float64(md.Time)
		for _, o := range outs {
			o.Flush(sl)
		}
	}
}
