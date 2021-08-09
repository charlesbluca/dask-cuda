import os.path
import sys
from json import dump, load
from statistics import mean
from time import perf_counter

import cupy

from dask.utils import format_bytes, format_time

import cudf
from cudf._lib.copying import pack, unpack

elements = int(sys.argv[1])
cols = int(sys.argv[2])
rows = elements // cols

df = cudf.DataFrame(cupy.zeros((rows, cols), dtype=int))
nbytes = df.memory_usage().sum()

pack_runs = []
unpack_runs = []
for _ in range(10):
    t1 = perf_counter()
    packed = pack(df)
    t2 = perf_counter()
    pack_runs.append(t2 - t1)

    t1 = perf_counter()
    unpack(packed)
    t2 = perf_counter()
    unpack_runs.append(t2 - t1)

    del packed

print("bytes: %s" % format_bytes(nbytes))
print("columns: %s" % cols)
print("wall clock: %s" % format_time(mean(pack_runs)))
print("throughput: %s/s" % format_bytes(nbytes / mean(pack_runs)))

pack_json = [
    {
        "nbytes": int(nbytes),
        "nbytes_str": format_bytes(nbytes),
        "columns": cols,
        "took": took,
        "throughput": float(nbytes / took),
    }
    for took in pack_runs
]

if os.path.isfile("linear_scaling.json"):
    with open("linear_scaling.json", "r") as f:
        l = load(f)
    l.extend(pack_json)
else:
    l = pack_json

with open("linear_scaling.json", "w") as f:
    dump(l, f, indent=2)

unpack_json = [
    {
        "nbytes": int(nbytes),
        "nbytes_str": format_bytes(nbytes),
        "columns": cols,
        "took": took,
        "throughput": float(nbytes / took),
    }
    for took in unpack_runs
]

if os.path.isfile("linear_scaling_unpack.json"):
    with open("linear_scaling_unpack.json", "r") as f:
        l = load(f)
    l.extend(unpack_json)
else:
    l = unpack_json

with open("linear_scaling_unpack.json", "w") as f:
    dump(l, f, indent=2)
