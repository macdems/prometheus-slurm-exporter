/* Copyright 2020 Victor Penso

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
	"io"
	"log"
	"os/exec"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

func QosJobsData(state string, format string) []byte {
	cmd := exec.Command("squeue", "-a", "-r", "-h", "-o"+format, "--states="+state)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := io.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

type QosMetrics struct {
	cpus_allocated float64
	jobs_running   float64
	jobs_pending   float64
	jobs_total     float64
}

func ParseQosMetrics() map[string]*QosMetrics {
	qoses := make(map[string]*QosMetrics)

	running_lines := strings.Split(string(QosJobsData("RUNNING", "%q,%C")), "\n")
	for _, line := range running_lines {
		if strings.Contains(line, ",") {
			split_line := strings.Split(line, ",")
			qos := split_line[0]
			if qos == "" {
				continue
			}
			_, key := qoses[qos]
			if !key {
				qoses[qos] = &QosMetrics{0, 0, 0, 0}
			}
			cpus, _ := strconv.ParseFloat(strings.Split(line, ",")[1], 64)
			qoses[qos].cpus_allocated += cpus
			qoses[qos].jobs_running += 1
			qoses[qos].jobs_total += 1
		}
	}

	pending_lines := strings.Split(string(QosJobsData("PENDING", "%q")), "\n")
	for _, qos := range pending_lines {
		if qos == "" {
			continue
		}
		_, key := qoses[qos]
		if !key {
			qoses[qos] = &QosMetrics{0, 0, 0, 0}
		}
		qoses[qos].jobs_pending += 1
		qoses[qos].jobs_total += 1
	}

	return qoses
}

type QosCollector struct {
	cpus_allocated *prometheus.Desc
	jobs_running   *prometheus.Desc
	jobs_pending   *prometheus.Desc
	jobs_total     *prometheus.Desc
}

func NewQosCollector() *QosCollector {
	labels := []string{"qos"}
	return &QosCollector{
		cpus_allocated: prometheus.NewDesc("slurm_qos_cpus_allocated", "Allocated CPUs for QOS", labels, nil),
		jobs_running:   prometheus.NewDesc("slurm_qos_jobs_running", "Running jobs for QOS", labels, nil),
		jobs_pending:   prometheus.NewDesc("slurm_qos_jobs_pending", "Pending jobs for QOS", labels, nil),
		jobs_total:     prometheus.NewDesc("slurm_qos_jobs_total", "Total jobs for QOS", labels, nil),
	}
}

func (pc *QosCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.cpus_allocated
	ch <- pc.jobs_running
	ch <- pc.jobs_pending
	ch <- pc.jobs_total
}

func (pc *QosCollector) Collect(ch chan<- prometheus.Metric) {
	pm := ParseQosMetrics()
	for p := range pm {
		if pm[p].cpus_allocated > 0 {
			ch <- prometheus.MustNewConstMetric(pc.cpus_allocated, prometheus.GaugeValue, pm[p].cpus_allocated, p)
		}
		if pm[p].jobs_running > 0 {
			ch <- prometheus.MustNewConstMetric(pc.jobs_running, prometheus.GaugeValue, pm[p].jobs_running, p)
		}
		if pm[p].jobs_pending > 0 {
			ch <- prometheus.MustNewConstMetric(pc.jobs_pending, prometheus.GaugeValue, pm[p].jobs_pending, p)
		}
		if pm[p].jobs_total > 0 {
			ch <- prometheus.MustNewConstMetric(pc.jobs_total, prometheus.GaugeValue, pm[p].jobs_total, p)
		}
	}
}
