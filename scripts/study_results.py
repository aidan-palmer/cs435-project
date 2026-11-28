"""Read a generated dashboard bundle and its registered allocation experiments."""
import json
from pathlib import Path

if __package__:
    from .allocation_results import AllocationResults
else:
    from allocation_results import AllocationResults

DEFAULT_RESULTS=Path(__file__).resolve().parents[1]/'data/processed/dashboard-results'


class StudyResults:
    DOWNLOADS={'forecast':'forecast_accuracy.csv','allocation':'allocation_comparison.csv',
               'runtime':'runtime.csv','snapshot':'results.json','scaling':'scaling.csv'}

    def __init__(self,directory=DEFAULT_RESULTS,repo=None):
        self.directory=Path(directory)
        self.repo=Path(repo or Path(__file__).resolve().parents[1]).resolve()
        self._stamp=None;self._data=None;self._series=None;self._registry=None
        self._allocations={}

    def _read(self):
        path=self.directory/'results.json'
        if not path.is_file():return False
        stamp=tuple((p.stat().st_mtime_ns,p.stat().st_size) for p in
                    [path,self.directory/'forecast_series.json',self.directory/'experiments.json'])
        if stamp!=self._stamp:
            self._data=json.loads(path.read_text())
            self._series=json.loads((self.directory/'forecast_series.json').read_text())
            self._registry=json.loads((self.directory/'experiments.json').read_text())
            self._allocations={};self._stamp=stamp
        return True

    def view(self):
        if not self._read():return dict(available=False)
        return dict(self._data,available=True)

    def forecast(self,building,day):
        if not self._read():raise ValueError('No study results are loaded')
        try:return self._series[building][day]
        except KeyError:raise ValueError('Choose a building and UTC date from the matched pilot') from None

    def allocation(self,identifier):
        if not self._read() or identifier not in self._registry:raise ValueError('Unknown saved experiment')
        path=(self.repo/self._registry[identifier]).resolve()
        allowed=[self.repo/'outputs/priority-experiments',self.repo/'outputs/final-study-v1/evaluation']
        if not any(path.is_relative_to(root) for root in allowed):raise ValueError('Experiment is outside the registered results folder')
        if identifier not in self._allocations:self._allocations[identifier]=AllocationResults(path)
        return self._allocations[identifier]

    def download(self,name):
        if name not in self.DOWNLOADS or not self._read():raise ValueError('Unknown results download')
        filename=self.DOWNLOADS[name]
        path=self.directory/filename
        if not path.is_file():raise ValueError('That results download is not available')
        return filename,path.read_bytes()
