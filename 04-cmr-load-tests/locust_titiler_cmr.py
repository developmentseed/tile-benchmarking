from locust import HttpUser, task, events
import pandas as pd


urls = []


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--urls-file", type=str, required=True, help="File of URLs separated by lines"
    )


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    # Extract the 'urls' argument value
    query_csv = environment.parsed_options.urls_file
    df = pd.read_csv(query_csv, header=None, names=["query"])
    urls.extend(df["query"].to_list())


class TitilerCmrUser(HttpUser):
    @task
    def get_tile(self):
        for url in urls:
            self.client.get(url)
