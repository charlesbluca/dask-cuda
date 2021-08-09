#!/bin/bash

for size in 4000000 8000000 16000000 32000000
do
    for frac in 1 2 4
    do
        if [[ $frac -eq 1 ]]
        then
            DASK_JIT_UNSPILL=1 UCX_MAX_RNDV_RAILS=1 UCX_MEMTYPE_REG_WHOLE_ALLOC_TYPES=cuda python local_cudf_merge.py -d 0,1,2,3 -p ucx --enable-tcp-over-ucx --enable-infiniband --enable-nvlink --enable-rdmacm --interface ib0 -c $size --benchmark-json rows_"$size"_no_limit --runs 5
        else
            let "limit = ($size * 32) / $frac"
            DASK_JIT_UNSPILL=1 UCX_MAX_RNDV_RAILS=1 UCX_MEMTYPE_REG_WHOLE_ALLOC_TYPES=cuda python local_cudf_merge.py -d 0,1,2,3 -p ucx --enable-tcp-over-ucx --enable-infiniband --enable-nvlink --enable-rdmacm --interface ib0 -c $size --device-memory-limit $limit --benchmark-json rows_"$size"_limit_"$limit"_bytes --runs 5
        fi
    done
done
