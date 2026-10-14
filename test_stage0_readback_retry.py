import unittest
from unittest.mock import Mock,patch
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

class TestReadbackRetries(unittest.TestCase):
    def make_subject(self, replies):
        subject=Stage0_Daily_Ingestor.__new__(Stage0_Daily_Ingestor)
        subject._verify=Mock(side_effect=replies)
        return subject

    @patch("stage0_ingestor.time.sleep")
    def test_two_transient_sheet_api_errors_then_success(self,sleep):
        x=self.make_subject([
            Stage0Error("SHEET_READ_FAILED","Raw_OHLCV: APIError"),
            Stage0Error("AUTHENTICATION_FAILED","APIError"),
            {"Raw_OHLCV":{"status":"PASS"}}
        ])
        result=x._verify_with_transient_retries({})
        self.assertEqual(result["Raw_OHLCV"]["status"],"PASS")
        self.assertEqual(x._verify.call_count,3)
        self.assertEqual(sleep.call_count,2)

    @patch("stage0_ingestor.time.sleep")
    def test_transient_readback_mismatch_one_retry(self,sleep):
        x=self.make_subject([
            Stage0Error("VERIFICATION_FAILED","Raw_OHLCV: row count mismatch"),
            {"Raw_OHLCV":{"status":"PASS"}}
        ])
        self.assertEqual(x._verify_with_transient_retries({})["Raw_OHLCV"]["status"],"PASS")
        self.assertEqual(sleep.call_count,1)

    @patch("stage0_ingestor.time.sleep")
    def test_permanent_provenance_error_never_retried(self,sleep):
        x=self.make_subject([Stage0Error("SCHEMA_FAILED","Bad sheet header")])
        with self.assertRaises(Stage0Error):
            x._verify_with_transient_retries({})
        self.assertEqual(x._verify.call_count,1)
        sleep.assert_not_called()

    @patch("stage0_ingestor.time.sleep")
    def test_permanent_sheets_api_failure_remains_blocked(self,sleep):
        x=self.make_subject([
            Stage0Error("SHEET_READ_FAILED","Raw_OHLCV: APIError")
            for _ in range(4)
        ])
        with self.assertRaises(Stage0Error):
            x._verify_with_transient_retries({})
        self.assertEqual(x._verify.call_count,4)
        self.assertEqual(sleep.call_count,3)

    @patch("stage0_ingestor.time.sleep")
    def test_auth_error_not_from_api_is_not_retryable(self,sleep):
        x=self.make_subject([Stage0Error("AUTHENTICATION_FAILED","InvalidKey")])
        with self.assertRaises(Stage0Error):
            x._verify_with_transient_retries({})
        sleep.assert_not_called()

if __name__=="__main__":unittest.main()
