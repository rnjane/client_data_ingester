# Multiply Client Data Ingester
## Intro
This is a toy application that mimics a part of Multiply's functionality, bulk-ingesting product data from our clients.
At Multiply, we often have to get information from our clients regarding the products they are selling e.g. the name
of the product, certain identifying information as well as pricing information.

While this implementation only has the most basic features, it is meant to be well engineered so that the little that
has been built is built well. As your interview assignment, you will be required to implement a few features in this 
application. 

## Setting yourself up
To set yourself up, you need to:

1. Clone the codebase to your local machine
2. Install dependencies ([Python3](https://www.python.org/downloads/), [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer), [docker compose](https://docs.docker.com/compose/install/))
3. At this point, the commands `python3 --version`, `poetry --version` and `docker compose --version` should work on your terminal
4. On the repository, `cd mply_ingester/backend; poetry install`
5. (Assuming you're still on the backend directory) Run the tests using `poetry run pytest mply_ingester/tests`

All tests except 1 should pass and your dev environment is ready to go. The failing test will pass if you do this 
exercise well


## The task
Look at the test `test_products.py`. This test shows the main functionality that exists. Currently, we can ingest client_data
from a csv file.

Below is the backlog of tasks that we have:

1. Currently, our system only supports creating new products. We would like to be able to submit a file and any skus that dont exist are created and the ones that exist are updated (This should fix the failing test) <br />
&emsp; i. In Update mode, any value that is not supplied should not be touched e.g. if I only supply sku and title, only update the title (SKU can never be updated) <br />
&emsp; ii. In update mode we should also update the value of `last_changed_on` for the client_product <br />
2. We have a very big client who wants to supply their client data in json format. Update our codebase so we can support this <br />
3. Add a full update mode where we ingest a number of products. Any product ingested is assumed to be active and any product that was absent is assumed to be inactive


You are required to do task 1 and any one other task between task 2 and 3.

Your implementation should include appropriate tests. You are allowed to change any part of the code you 
deem necessary.
