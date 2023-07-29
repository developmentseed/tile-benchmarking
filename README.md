# tile-benchmarking

Home for data generation and scripts for benchmarking tile servers. 

Focused on titiler-xarray and titiler-pgstac at this time.

# What's here

## e2e

The `e2e/` directory includes scripts for testing a cloud-deployed instance of titiler-xarray end-to-end on various zarr datasets. The URL being tested at this time is https://dev-titiler-xarray.delta-backend.com and is a cloud-hosted version of [titiler-xarray](https://github.com/developmentseed/titiler-xarray).

## profiling

The `profiling/` directory includes scripts for generating test data and running performance testing on the `XarrayReader` code. The goal is to understand, without conflation with any network latencies, how the tiling code performs on different datasets and tile sets.

To do this profiling, it includes copies of the code in titiler-xarray and titiler-pgstac so that code blocks can be wrapped in timers and cprofile and other library logs can be added.

The profiling code also includes profileing of titiler-pgstac which depends on a pgSTAC database deployed and populated with items.

More details about how the test data is generated can be found in [`profiling/README.md`](./profiling/README.md).

Details on how pgSTAC is deployed can be found in [.github/workflows/deploy.yml](.github/workflows/deploy.yml) for steps.
