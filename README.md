# tile-benchmarking

Home for data generation and scripts for benchmarking tile servers. 

Focused on titiler-xarray and titiler-pgstac at this time.

# Deploying pgSTAC with CDK

```bash
nvm use 
npm install
npm run build
# if necessary, run cdk bootstrap
cdk deploy --all
```