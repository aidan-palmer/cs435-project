"""Record measured phase times with configuration and local environment."""
import json
import hashlib
from importlib import metadata
import os
from pathlib import Path
import platform
import subprocess
import sys
from datetime import datetime, timezone


def save_runtime(path, phases, configuration, spark=None):
    repo=Path(__file__).resolve().parents[1]
    record=dict(recorded_at_utc=datetime.now(timezone.utc).isoformat(),
                phases_seconds=phases,configuration=configuration,
                environment=dict(python=sys.version,platform=platform.platform(),
                                 machine=platform.machine(),logical_cpus=os.cpu_count()),
                git_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
                scope='One measured local run; no distributed speedup or repeated-run uncertainty established')
    record['code_sha256']={str(p.relative_to(repo)):hashlib.sha256(p.read_bytes()).hexdigest()
                           for p in sorted((repo/'scripts').glob('*.py'))}
    record['environment']['packages']={}
    for package in ['numpy','pandas','pyarrow','pyspark','tensorflow','keras']:
        try:record['environment']['packages'][package]=metadata.version(package)
        except metadata.PackageNotFoundError:pass
    if spark is not None:
        record['environment'].update(spark=spark.version,master=spark.sparkContext.master,
            default_parallelism=spark.sparkContext.defaultParallelism,
            shuffle_partitions=spark.conf.get('spark.sql.shuffle.partitions'))
    out=Path(path);out.parent.mkdir(parents=True,exist_ok=True)
    with out.open('x') as stream:json.dump(record,stream,indent=2,allow_nan=False)
