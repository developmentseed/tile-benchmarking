from locust import HttpUser, task, events
import locust_plugins
import argparse
import os

# locust --urls-file=urls/CMIP6_GISS-E2-1-G_historical_urls.txt -i 100

# Function to replace environment variables with their values in a URL
def replace_variables(url, variables):
    path = url
    for key, value in variables.items():
        if key == "HOST":
            path = path.replace(f"$({key})", '')
        else:
            path = path.replace(f"$({key})", value)
    return path


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--urls-file", type=str, required=True, help="File of URLs separated by lines")

paths = []
variables = {}
@events.init.add_listener
def on_test_start(environment, **kwargs):
    print("A new test is starting")
    
    # Extract the 'urls' argument value
    file_path = environment.parsed_options.urls_file

    with open(file_path, 'r') as file:
        lines = file.readlines()

        # Set environment variables from the first 5 lines
        for i in range(4):
            key, value = lines[i].strip().split('=', 1)
            print(f"Setting variable {key} to {value}")
            variables[key] = value

        # Replace environment variables in the paths and append to the list
        for url in lines[4:]:
            paths.append(replace_variables(url.strip(), variables))

@events.quitting.add_listener
def _(environment, **kw):
    environment.process_exit_code = 0

class TilesUser(HttpUser):
    @task
    def test_tiles(self):
        print("Running task")
        for path in paths:
            self.client.get(path)
