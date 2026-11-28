import unittest
import pandas as pd
from scripts.final_allocation import freeze_profile


class ProspectiveProtocolTest(unittest.TestCase):
    def fixture(self):
        decision=pd.Timestamp('2018-11-08T00:00Z')
        old=pd.DataFrame(dict(building_id=['A']*672,target_timestamp=pd.date_range(decision-pd.Timedelta(days=28),periods=672,freq='h'),predicted_kwh=[2.]*672))
        old['forecast_available_at']=old.target_timestamp.dt.floor('D')
        future=pd.DataFrame(dict(building_id=['A']*24,target_timestamp=pd.date_range(decision,periods=24,freq='h'),demand_kwh=[999.]*24,solar_generation_kwh=[3.]*24))
        return old,future,decision

    def test_future_actuals_cannot_change_frozen_predictions(self):
        old,future,decision=self.fixture()
        first=freeze_profile(old,future,decision)
        second=freeze_profile(old,future.assign(demand_kwh=0),decision)
        self.assertTrue(first.predicted_kwh.eq(2).all())
        self.assertTrue(first.predicted_kwh.equals(second.predicted_kwh))

    def test_missing_late_and_duplicate_planning_records_fail(self):
        old,future,decision=self.fixture()
        for bad in [old.iloc[:-1],pd.concat([old,old.iloc[:1]]),old.assign(forecast_available_at=decision+pd.Timedelta(hours=1))]:
            with self.assertRaises(ValueError):freeze_profile(bad,future,decision)
        bad=old.copy();bad.loc[0,'target_timestamp']=decision
        with self.assertRaises(ValueError):freeze_profile(bad,future,decision)


if __name__=='__main__':unittest.main()
