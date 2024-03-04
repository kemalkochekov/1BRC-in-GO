# 1 Billion Row Challange in GO

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Generating Tests](#generating-tests)
- [Running the Application](#running-the-application)
- [Result](#result)
- [Benchmark Result](#benchmark-result)

## Introduction

This project is an implementation of the 1 Billion Row Challenge in Go. It involves generating and processing a large dataset of one billion rows efficiently using Go programming language.

## Prerequisites

Before running this application, ensure that you have the following prerequisites installed:

- Go: [Install Go](https://go.dev/doc/install/)
- Python3[Install Python3](https://www.python.org/downloads/)

## Installation

1. Clone this repository
  ```bash
    git clone https://github.com/kemalkochekov/1BRC-in-GO.git
  ```
2. Navigate to the project directory:
  ```
    cd 1BRC-in-GO
  ```

## Generating Tests

Before running the application, generate tests using the provided Python script:
```bash
python3 create_measurements.py 1000000000
```

## Running the Application

To run the application, execute the following command:
```bash
go run cmd/main.go
```

## Result
After running the application, the achieved runtime for processing 1 billion rows is approximately 21.064296708 seconds.
![IMAGE 2024-03-05 00:42:58](https://github.com/kemalkochekov/1BRC-in-GO/assets/85355663/07fd058b-2e79-4eeb-9179-11d15c1794d3)

## Benchmark Result

Benchmarking the application yields a runtime of approximately 21.99330325 seconds.

![IMAGE 2024-03-05 00:46:21](https://github.com/kemalkochekov/1BRC-in-GO/assets/85355663/b73d1559-0aaf-4a09-bc59-477ba4b1d9a8)
