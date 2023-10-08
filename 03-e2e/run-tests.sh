mkdir -p results
for file in urls/*; do
  echo "Running tests for $file"
  locust -i 10 --urls-file="$file" --csv=results/$(basename ${file%.*}) --csv-full-history
done