# End-to-end Testing

End to end testing refers to tests which report performance of requsts to tiling APIs via HTTP requests.

Commands below are run within this directory.

Tests can be run using [https://locust.io/](https://locust.io/) or [siege](https://www.joedog.org/siege-home/).

## Environment Setup

```bash
# It's recommanded to use virtual environment
cd e2e
python -m pip install --upgrade virtualenv
virtualenv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Generating URLs to Test

Note: for the FWI-GEOS-5-Hourly dataset (or any dataset in veda-data-store and veda-data-store-staging), the `gen_test_urls.py` script requires data access via a role from the SMCE VEDA AWS account. Please skip this dataset or contact the SMCE team for access.

If you have role-based access to those buckets, you will need to assume the role using MFA and assume that role.

Then set the following environment variables:

```bash
AWS_ACCESS_KEY_ID=XXX
AWS_SECRET_ACCESS_KEY=XXX
AWS_SESSION_TOKEN=XXX
```

Otherwise, that dataset will just be skipped in the `gen_test_urls.py` script via a try/catch statement.

```bash
mkdir -p urls
python gen_test_urls.py
```

## Run Locust

```bash
./run-all.sh
```

## Run siege

You can also run tests using siege.

```bash
siege -f urls/CMIP6_GISS-E2-1-G_historical_urls.txt -c4 -r25 -l
siege -f urls/gpm3imergdl_urls.txt -c4 -r25 -l
# ... so on
```

## Read results

[`read-results.ipynb`](./read-results.ipynb) is a Jupyter notebook that reads the results CSV files.
