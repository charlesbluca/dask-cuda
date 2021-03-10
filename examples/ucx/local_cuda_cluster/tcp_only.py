from dask.distributed import Client
from dask_cuda import LocalCUDACluster
from dask_cuda.initialize import initialize


def main():
    cluster = LocalCUDACluster(
        protocol="ucx",
        interface="enp1s0f0", # passed to the scheduler
        enable_tcp_over_ucx=True,
        rmm_pool_size="1GB",
    )

if __name__ == "__main__":
    main()