# End-to-end Testing

End to end testing refers to tests which report performance of requsts to tiling APIs via HTTP requests.

Commands below are run within this directory.

Tests can be run using [https://locust.io/](https://locust.io/) or [siege](https://www.joedog.org/siege-home/).

## Environment Setup

See README at the root of this repository.

## Set credentials

Scripts reference files in s3://nasa-eodc-data-store and requires data access via a role from the SMCE VEDA AWS account.

YOu can either set environment variables for access yourself or, if logged into the VEDA JupyterHub you can run:

```
python helpers/eodc_hub_role.py
```

and use the output to set credentials.

## Generating URLs to Test

You can skip this step if the urls/ directory is already populated and you are not trying to override existing datasets.

```bash
mkdir -p urls
python gen_test_urls.py --env prod|dev
```

## Run Locust

See [.github/workflows/run-benchmarks.yml](../.github/workflows/run-benchmarks.yml) for an example of how to run locust.

The workflow runs for both prod and dev so results can be compared, as demonstrated in [compare-prod-dev.ipynb](./compare-prod-dev.ipynb).

### Read results

[`read-results.ipynb`](./read-results.ipynb) is a Jupyter notebook that reads the results CSV files.

## Run siege

You can also run tests using siege.

```bash
for file in $(find urls -name "*.txt" -type f); do
  siege -f "$file" --concurrent=4 --reps=10 
done

siege -f urls/600_1440_1_CMIP6_daily_GISS-E2-1-G_tas.zarr_urls.txt --concurrent=4 --reps=10 -l \ 600_1440_1_CMIP6_daily_GISS-E2-1-G_tas.zarr_urls.txt.out
```
