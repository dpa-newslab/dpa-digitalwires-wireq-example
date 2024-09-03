# Import dpa-digitalwires via wireQ-API

This repository shows best practices of how to import the dpa-digitalwires
format via wireQ-API and stores the received articles to the local file system.

There are two approaches:
1. [Fast and efficient retrieval of all new messages via POST request `dequeue_entries.json`](#dequeue)
2. [Fast and efficient retrieval of all new messages via GET and DELETE request](#get-and-delete)

## Requirements

This setup was tested with:

* >=Python3.9

Install requirements by calling:

```
pip install -r requirements.txt
```

## How to receive articles in dpa-digitalwires via wireQ-API

The python implementation can be found in `how_to_receive_wireq.py`.
This is basically an executable test which shows how to receive articles in the
dpa-digitalwires format via wireq-API.

Open your terminal and type:
```
export BASE_URL=https://...
python how_to_receive_wireq.py
```

## DEQUEUE

The python implementation can be found in `lib/dequeue_import.py`.
Please copy the `base-URL` from your setup in the [API-Portal](https://api-portal.dpa-newslab.com).
In the given example code the environment variable `BASE_URL` is used.

Open your terminal and type:
```
export BASE_URL=https://...
npm run dequeue-import
```

## GET and DELETE

The python implementation can be found in `lib/get_delete_import.py`.
Please copy the `base-URL` from your setup in the [API-Portal](https://api-portal.dpa-newslab.com).
In the given example code the environment variable `BASE_URL` is used.

Open your terminal and type:
```
export BASE_URL=https://...
npm run get-delete-import
```

# The wireQ-Fake local server

To test wireQ-related functions and products, it can be impractical to wait on new articles in a live wireQ.
As a solution, a customizable wireQ-fake can be found in the `/test` directory. 
To use the wireQ-Fake, create a `.env` file in the `/test` directory by adapting the `.env.example`. The 
values in the `.env.example` are useful baseline values to use in your own `.env`.

**NOTE**: To run these tests using the wireQ-Fake, you cannot have the BASE_URL system variable set via 
`export BASE_URL=https://...`

## Starting the wireQ-fake:
```
npm install
npm run start-localserver
```

The wireQ-fake should now run under http://127.0.0.1:8080

## Running wireQ-fake tests:

If wireQ-fake is running:
```
npm run test-wireq
npm run test-mock
```

## Changing wireQ-fake parameters

Parameters for the wireQ-fake and its tests can be changed via `/test/.env`. If testing of a real wireQ is required, 
the `BASE_URL` can be changed to a real wireQ-endpoint.

**NOTE**: These changes only impact `npm run test-wireq` and not `npm run test-mock`, which uses a different, 
local version of the wireQ-fake.

## Using the wireQ-fake with your own tests

- Start the wireQ-fake\
- Change the Base-URL in your test to http://127.0.0.1:8080
- Run the test
