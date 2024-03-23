/* Copyright 2021 Victor Penso

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
    "io/ioutil"
    "os/exec"
    "log"
    "strings"
    "strconv"
    "github.com/prometheus/client_golang/prometheus"
)

func FairShareData() []byte {
    cmd := exec.Command("sshare", "-n", "-P", "-o", "user,account,fairshare", "-U", "-a", "-A", "qchem,photonics")
    stdout, err := cmd.StdoutPipe()
    if err != nil {
        log.Fatal(err)
    }
    if err := cmd.Start(); err != nil {
        log.Fatal(err)
    }
    out, _ := ioutil.ReadAll(stdout)
    if err := cmd.Wait(); err != nil {
        log.Fatal(err)
    }
    return out
}

type FairShareMetrics struct {
    fairshare float64
}

type UserAccount struct {
    user string
    account string
}

func ParseFairShareMetrics() map[UserAccount]*FairShareMetrics {
    metrics := make(map[UserAccount]*FairShareMetrics)
    lines := strings.Split(string(FairShareData()), "\n")
    for _, line := range lines {
        if ! strings.HasPrefix(line,"  ") {
            if strings.Contains(line,"|") {
                items := strings.Split(line,"|")
                user := strings.Trim(items[0]," ")
                account := strings.Trim(items[1]," ")
                user_account := UserAccount{user, account}
                _, key := metrics[user_account]
                fairshare, _ := strconv.ParseFloat(items[2],64)
                if !key {
                    metrics[user_account] = &FairShareMetrics{fairshare}
                } else {
                    metrics[user_account].fairshare = fairshare
                }
            }
        }
    }
    return metrics
}

type FairShareCollector struct {
    fairshare *prometheus.Desc
}

func NewFairShareCollector() *FairShareCollector {
    labels := []string{"user", "account"}
    return &FairShareCollector{
        fairshare: prometheus.NewDesc("slurm_user_fairshare", "FairShare for user", labels, nil),
    }
}

func (fsc *FairShareCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- fsc.fairshare
}

func (fsc *FairShareCollector) Collect(ch chan<- prometheus.Metric) {
    fsm := ParseFairShareMetrics()
    for f := range fsm {
        ch <- prometheus.MustNewConstMetric(fsc.fairshare, prometheus.GaugeValue, fsm[f].fairshare, f.user, f.account)
    }
}
