# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd
"""Runs the RocksDB Shinjuku experiments.

This script runs the RocksDB Shinjuku experiments on ghOSt and on CFS. In these
experiments, there is a centralized FIFO queue maintained for RocksDB requests.
For ghOSt, long requests that exceed their time slice are preempted so that they
do not prevent short requests from running (i.e., ghOSt prevents head-of-line
blocking). The preempted requests are added to the back of the FIFO. For CFS,
requests are run to completion.
"""

from typing import Sequence
from absl import app
from experiments.scripts.options import CheckSchedulers, PrintFormat
from experiments.scripts.options import GetGhostOptions
from experiments.scripts.options import GetRocksDBOptions
from experiments.scripts.options import Scheduler
from experiments.scripts.run import Experiment
from experiments.scripts.run import Run

# 4.2: 20 spinning workers threads and 1 load generator
_NUM_CPUS = 21

_NUM_CFS_WORKERS = _NUM_CPUS - 2
_NUM_GHOST_WORKERS = 200


def RunCfs():
  """Runs the CFS (Linux Completely Fair Scheduler) experiment."""
  e: Experiment = Experiment()
  # Run throughputs 10000, 20000, 30000, and 40000.
  e.throughputs = list(i for i in range(10000, 50000, 10000))
  # Toward the end, run throughputs 50000, 51000, 52000, ..., 80000.
  e.throughputs.extend(list(i for i in range(50000, 81000, 1000)))
  e.rocksdb = GetRocksDBOptions(Scheduler.CFS, _NUM_CPUS, _NUM_CFS_WORKERS)
  e.rocksdb.range_query_ratio = 0.005
  e.antagonist = None
  e.ghost = None

  Run(e)


def RunGhost():
  """Runs the ghOSt experiment."""
  e: Experiment = Experiment()
  
  e.rocksdb = GetRocksDBOptions(Scheduler.GHOST, _NUM_CPUS, _NUM_GHOST_WORKERS)
  e.antagonist = None
  e.ghost = GetGhostOptions(_NUM_CPUS)
  
  e.rocksdb.experiment_duration = "10s"
  
  # 99.5% 4us, 0.5% 10000us, 30us
  # e.throughputs = list(i for i in range(10000, 250000, 10000))
  # e.throughputs.extend(list(i for i in range(250000, 285000, 2500)))
  # e.throughputs.extend(list(i for i in range(285000, 286000, 100)))
  # e.rocksdb.range_query_ratio = 0.005
  # e.rocksdb.print_ns = True
  # e.rocksdb.get_duration = "4us"
  # e.rocksdb.range_duration = "10ms"
  # e.ghost.preemption_time_slice = '30us'
  
  # 99.5% 0.5us, 0.5 500us, 30us
  # e.throughputs = list(i for i in range(100000, 1900000, 100000))
  # e.throughputs.extend(list(i for i in range(1900000, 2000000, 10000)))
  # e.rocksdb.range_query_ratio = 0.005
  # e.rocksdb.print_ns = True
  # e.rocksdb.get_duration = "0.5us"
  # e.rocksdb.range_duration = "500us"
  # e.ghost.preemption_time_slice = '30us'
  
  # 50% 1us, 50% 100us, 30us
  # e.throughputs = list(i for i in range(50000, 300000, 50000))  
  # e.throughputs.extend(list(i for i in range(250000, 320000, 10000)))
  # e.rocksdb.range_query_ratio = 0.5
  # e.rocksdb.print_ns = True
  # e.rocksdb.get_duration = "1us"
  # e.rocksdb.range_duration = "100us"
  # e.ghost.preemption_time_slice = '30us'
  
  # 100% 1us 30us
  # e.throughputs = list(i for i in range(100000, 2000000, 100000))
  # e.throughputs.extend(list(i for i in range(1900000, 2000000, 10000)))
  # e.rocksdb.range_query_ratio = 0
  # e.rocksdb.print_ns = True
  # e.rocksdb.get_duration = "1us"
  # e.ghost.preemption_time_slice = '30us'
  
  # 50% 1us, 50% 100us, 20us
  # e.throughputs = list(i for i in range(50000, 300000, 50000))  
  # e.throughputs.extend(list(i for i in range(250000, 320000, 10000)))
  # e.rocksdb.range_query_ratio = 0.5
  # e.rocksdb.print_ns = True
  # e.rocksdb.get_duration = "1us"
  # e.rocksdb.range_duration = "100us"
  # e.ghost.preemption_time_slice = "20us"


  # 50% 1us, 50% 100us, 10us
  # e.throughputs = list(i for i in range(50000, 300000, 50000))  
  # e.throughputs.extend(list(i for i in range(250000, 320000, 10000)))
  # e.rocksdb.range_query_ratio = 0.5
  # e.rocksdb.print_ns = True
  # e.rocksdb.get_duration = "1us"
  # e.rocksdb.range_duration = "100us"
  # e.ghost.preemption_time_slice = "10us"

  # 50% 1us, 50% 100us, 5us
  e.throughputs = list(i for i in range(50000, 300000, 50000))  
  e.throughputs.extend(list(i for i in range(250000, 320000, 10000)))
  e.rocksdb.range_query_ratio = 0.5
  e.rocksdb.print_ns = True
  e.rocksdb.get_duration = "1us"
  e.rocksdb.range_duration = "100us"
  e.ghost.preemption_time_slice = "5us"

  Run(e)


def main(argv: Sequence[str]):
  if len(argv) > 3:
    raise app.UsageError('Too many command-line arguments.')
  elif len(argv) == 1:
    raise app.UsageError(
        'No experiment specified. Pass `cfs` and/or `ghost` as arguments.')

  # First check that all of the command line arguments are valid.
  if not CheckSchedulers(argv[1:]):
    raise ValueError('Invalid scheduler specified.')

  # Run the experiments.
  for i in range(1, len(argv)):
    scheduler = Scheduler(argv[i])
    if scheduler == Scheduler.CFS:
      RunCfs()
    else:
      if scheduler != Scheduler.GHOST:
        raise ValueError(f'Unknown scheduler {scheduler}.')
      RunGhost()


if __name__ == '__main__':
  app.run(main)
