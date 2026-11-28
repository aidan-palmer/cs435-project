"""Export held-out forecasts from a saved LSTM without retraining it."""
import argparse
import hashlib
import json
from pathlib import Path
import time

import tensorflow as tf

if __package__:
    from .train_lstm import evaluate_and_save
    from .runtime_evidence import save_runtime
else:
    from train_lstm import evaluate_and_save
    from runtime_evidence import save_runtime


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sequences',type=Path,help='Local partitioned sequence directory')
    parser.add_argument('model',type=Path,help='Directory containing model.keras and normalization.json')
    parser.add_argument('output',type=Path,help='New directory for predictions and metrics')
    parser.add_argument('--batch-size',type=int,default=128)
    args=parser.parse_args()
    if args.batch_size<1:parser.error('Batch size must be positive')
    if args.output.exists():parser.error('Choose a new output directory')
    started=time.perf_counter()
    model=tf.keras.models.load_model(args.model/'model.keras',compile=False)
    if model.input_shape[1]!=168 or model.input_shape[2] not in (1,2) or model.output_shape[-1]!=24:
        raise ValueError('Expected a 168-hour, one/two-channel, 24-target model')
    stats=json.loads((args.model/'normalization.json').read_text())
    loaded=time.perf_counter()-started
    args.output.mkdir(parents=True)
    started=time.perf_counter()
    metrics=evaluate_and_save(model,args.sequences/'split=test',stats,model.input_shape[2]==2,args.output,args.batch_size)
    elapsed=time.perf_counter()-started
    (args.output/'metrics.json').write_text(json.dumps(metrics,indent=2))
    save_runtime(args.output/'runtime.json',dict(model_loading_seconds=loaded,prediction_metrics_and_export_seconds=elapsed),
        dict(sequences=str(args.sequences),model_directory=str(args.model),output=str(args.output),
             batch_size=args.batch_size,hourly_targets=metrics['num_predictions'],tensorflow=tf.__version__,
             model_sha256=hashlib.sha256((args.model/'model.keras').read_bytes()).hexdigest(),timestamp_clock='UTC'))
    print(json.dumps(metrics,indent=2))


if __name__=='__main__':main()
