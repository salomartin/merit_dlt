# Merit Aktiva DLT Source

This project implements a DLT (Data Load Tool) source for Merit Aktiva, allowing you to efficiently extract and load data from the Merit Aktiva API. The implementation supports both v1 and v2 API endpoints and handles rate limiting.

## Features

- Support for both Merit Aktiva API v1 and v2 endpoints
- Automatic rate limiting handling (60 requests per minute)
- Comprehensive error handling and retry mechanisms
- Support for all major Merit Aktiva data entities

## TODO

- Fix incremental loading implementation
  - Review and update date-based pagination logic
  - Handle edge cases for data changes
  - Add proper state management
- Add test suite
  - Unit tests for core functionality
  - Integration tests with API mocking
  - Test coverage for error handling scenarios

## Prerequisites

- Python 3.12 or higher
- `uv` package manager

## Installation

1. Clone the repository:
```bash
git clone https://github.com/salomartin/merit_dlt.git
cd merit_dlt
```

2. Set up a Python virtual environment using `uv`:
```bash
uv venv
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows
```

3. Install dependencies:
```bash
uv sync
```

## Configuration

1. Copy the example secrets file:
```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

2. Edit `.dlt/secrets.toml` and add your Merit Aktiva API credentials:
```toml
merit_api_key = "your-api-key"
merit_company_id = "your-company-id"
```

## Usage

To run a data pipeline:

```bash
uv run your_pipeline_script.py
```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request
