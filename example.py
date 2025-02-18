from typing import Any
import dlt
import os
from merit import aktiva

def main() -> None:
    os.environ["RUNTIME__LOG_LEVEL"] = "INFO"
    
    pipeline = dlt.pipeline(
        pipeline_name="aktiva",
        destination="duckdb",
        dataset_name="merit_aktiva",
    )


    load_info: Any = pipeline.run(aktiva()) # type: ignore
    print(load_info)

if __name__ == "__main__":
    main()
