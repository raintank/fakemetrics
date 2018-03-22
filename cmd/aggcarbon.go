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

	"github.com/spf13/cobra"
	"sync"

	"net"
	"os"
	"time"
)

// aggcarbonCmd represents the aggcarbon command
var aggcarbonCmd = &cobra.Command{
	Use:   "aggcarbon",
	Short: "Sends out a small set of metrics which you can test aggregation rules on",
	Run: func(cmd *cobra.Command, args []string) {
		conn, err := net.Dial("tcp", carbonAddr)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		initStats(true, "aggcarbon")

		to := time.Now().Unix()
		from := to - 6*24*60*60

		wg.Add(8)
		go do("fakemetrics.raw.min", conn, from, to)
		go do("fakemetrics.raw.max", conn, from, to)
		go do("fakemetrics.raw.all", conn, from, to)
		go do("fakemetrics.raw.default", conn, from, to)
		go do("fakemetrics.agg.min", conn, from, to)
		go do("fakemetrics.agg.max", conn, from, to)
		go do("fakemetrics.agg.all", conn, from, to)
		go do("fakemetrics.agg.default", conn, from, to)
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(aggcarbonCmd)
}

var wg sync.WaitGroup

func do(key string, conn net.Conn, from, to int64) {
	i := 0
	for ts := from; ts < to; ts++ {
		if ts%7200 == 0 {
			fmt.Println(key, ts-from, "/", to-from)
		}
		_, err := fmt.Fprintf(conn, "%s %d %d\n", key, i, ts)
		if err != nil {
			fmt.Println(err)
		}
		i++
	}
	wg.Done()
}
