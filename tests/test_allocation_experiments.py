from datetime import datetime
import unittest
import numpy as np
import pandas as pd
from scripts.allocation_experiments import match_predictions, predictions, score


class ModelAllocationTest(unittest.TestCase):
    def fixture(self):
        issued=pd.Timestamp('2018-11-08T23:00Z')
        return pd.DataFrame(dict(building_id=['A']*24,forecast_issued_at=[issued]*24,
            target_timestamp=pd.date_range(issued+pd.Timedelta(hours=1),periods=24,freq='h'),
            horizon=range(1,25),actual_kwh=[2.]*24,predicted_kwh=[1.]*24))

    def test_matched_24_hour_forecast_scores_physical_benefit(self):
        pred=predictions(self.fixture(),'UTC')
        target=pred[['building_id','target_timestamp']].assign(demand_kwh=2.,solar_generation_kwh=3.)
        merged=match_predictions(pred,target)
        result=score(merged,capacity=2,factor=.5).iloc[0]
        self.assertEqual(result.predicted_grid_reduction_kwh,24)
        self.assertEqual(result.realized_grid_reduction_kwh,36)
        self.assertEqual(result.realized_avoided_co2_kg,18)
        # Capacity changes overlap nonlinearly, not just total benefit metadata.
        self.assertEqual(score(merged,capacity=8).iloc[0].realized_grid_reduction_kwh,48)

    def test_missing_duplicate_wrong_actuals_and_future_issues_fail(self):
        pred=predictions(self.fixture(),'UTC')
        target=pred[['building_id','target_timestamp']].assign(demand_kwh=2.,solar_generation_kwh=3.)
        for other in [pred.iloc[:-1],pd.concat([pred,pred.iloc[:1]]),pred.assign(actual_kwh=3.)]:
            with self.assertRaises(ValueError):match_predictions(other,target)
        with self.assertRaisesRegex(ValueError,'unavailable'):
            match_predictions(pred,target,decision_time=pd.Timestamp('2018-11-08T23:00Z'))
        match_predictions(pred,target,decision_time=pd.Timestamp('2018-11-09T00:00Z'))
        with self.assertRaisesRegex(ValueError,'at or before'):
            match_predictions(pred,target,decision_time=pd.Timestamp('2018-11-10T00:00Z'))

    def test_nonfinite_partial_and_wrong_horizon_exports_fail(self):
        frame=self.fixture()
        for invalid in [frame.iloc[:-1],frame.assign(predicted_kwh=np.inf),frame.assign(horizon=1)]:
            with self.assertRaises(ValueError):predictions(invalid,'UTC')

    def test_est_conversion_and_negative_prediction_policy(self):
        pred=predictions(self.fixture().assign(predicted_kwh=-1.),'resstock-est-label')
        self.assertEqual(pred.target_timestamp.iloc[0],pd.Timestamp('2018-11-09T05:00Z'))
        frame=pred.assign(demand_kwh=2.,solar_generation_kwh=1.)
        self.assertEqual(score(frame).iloc[0].predicted_grid_reduction_kwh,0)
        self.assertTrue((frame.predicted_kwh<0).all())


if __name__=='__main__':unittest.main()
