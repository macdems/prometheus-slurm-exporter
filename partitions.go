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

func PartitionsData() []byte {
	cmd := exec.Command("sinfo", "-h", "-o%R,%C")
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

func PartitionsJobsData(state string) []byte {
	cmd := exec.Command("squeue", "-a", "-r", "-h", "-o%P", "--states="+state)
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

type PartitionMetrics struct {
	cpus_allocated float64
	cpus_idle      float64
	cpus_other     float64
	jobs_running   float64
	jobs_pending   float64
	jobs_total     float64
	cpus_total     float64
}

func ParsePartitionsMetrics() map[string]*PartitionMetrics {
	partitions := make(map[string]*PartitionMetrics)
	lines := strings.Split(string(PartitionsData()), "\n")
	for _, line := range lines {
		if strings.Contains(line, ",") {
			// name of a partition
			split_line := strings.Split(line, ",")
			partition := split_line[0]
			_, key := partitions[partition]
			if !key {
				partitions[partition] = &PartitionMetrics{0, 0, 0, 0, 0, 0, 0}
			}
			states := split_line[1]
			split_states := strings.Split(states, "/")
			allocated, _ := strconv.ParseFloat(split_states[0], 64)
			idle, _ := strconv.ParseFloat(split_states[1], 64)
			other, _ := strconv.ParseFloat(split_states[2], 64)
			total, _ := strconv.ParseFloat(split_states[3], 64)
			partitions[partition].cpus_allocated = allocated
			partitions[partition].cpus_idle = idle
			partitions[partition].cpus_other = other
			partitions[partition].cpus_total = total
		}
	}

	// get list of running jobs by partition name
	running_list := strings.Split(string(PartitionsJobsData("RUNNING")), "\n")
	for _, partition := range running_list {
		// accumulate the number of running jobs
		_, key := partitions[partition]
		if key {
			partitions[partition].jobs_running += 1
			partitions[partition].jobs_total += 1
		}
	}

	// get list of pending jobs by partition name
	pending_list := strings.Split(string(PartitionsJobsData("PENDING")), "\n")
	for _, partition := range pending_list {
		// accumulate the number of pending jobs
		_, key := partitions[partition]
		if key {
			partitions[partition].jobs_pending += 1
			partitions[partition].jobs_total += 1
		}
	}

	return partitions
}

type PartitionsCollector struct {
	cpus_allocated *prometheus.Desc
	cpus_idle      *prometheus.Desc
	cpus_other     *prometheus.Desc
	jobs_running   *prometheus.Desc
	jobs_pending   *prometheus.Desc
	jobs_total     *prometheus.Desc
	cpus_total     *prometheus.Desc
}

func NewPartitionsCollector() *PartitionsCollector {
	labels := []string{"partition"}
	return &PartitionsCollector{
		cpus_allocated: prometheus.NewDesc("slurm_partition_cpus_allocated", "Allocated CPUs for partition", labels, nil),
		cpus_idle:      prometheus.NewDesc("slurm_partition_cpus_idle", "Idle CPUs for partition", labels, nil),
		cpus_other:     prometheus.NewDesc("slurm_partition_cpus_other", "Other CPUs for partition", labels, nil),
		jobs_running:   prometheus.NewDesc("slurm_partition_jobs_running", "Running jobs for partition", labels, nil),
		jobs_pending:   prometheus.NewDesc("slurm_partition_jobs_pending", "Pending jobs for partition", labels, nil),
		jobs_total:     prometheus.NewDesc("slurm_partition_jobs_total", "Total jobs for partition", labels, nil),
		cpus_total:     prometheus.NewDesc("slurm_partition_cpus_total", "Total CPUs for partition", labels, nil),
	}
}

func (pc *PartitionsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.cpus_allocated
	ch <- pc.cpus_idle
	ch <- pc.cpus_other
	ch <- pc.jobs_running
	ch <- pc.jobs_pending
	ch <- pc.jobs_total
	ch <- pc.cpus_total
}

func (pc *PartitionsCollector) Collect(ch chan<- prometheus.Metric) {
	pm := ParsePartitionsMetrics()
	for p := range pm {
		if pm[p].cpus_allocated > 0 {
			ch <- prometheus.MustNewConstMetric(pc.cpus_allocated, prometheus.GaugeValue, pm[p].cpus_allocated, p)
		}
		if pm[p].cpus_idle > 0 {
			ch <- prometheus.MustNewConstMetric(pc.cpus_idle, prometheus.GaugeValue, pm[p].cpus_idle, p)
		}
		if pm[p].cpus_other > 0 {
			ch <- prometheus.MustNewConstMetric(pc.cpus_other, prometheus.GaugeValue, pm[p].cpus_other, p)
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
		if pm[p].cpus_total > 0 {
			ch <- prometheus.MustNewConstMetric(pc.cpus_total, prometheus.GaugeValue, pm[p].cpus_total, p)
		}
	}
}
