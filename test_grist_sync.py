import io
import urllib.error
import unittest
from unittest.mock import MagicMock, patch

from grist_sync import put_batch


class GristRetryTest(unittest.TestCase):
    @patch("grist_sync.time.sleep")
    @patch("grist_sync.urllib.request.urlopen")
    def test_retries_a_timeout_then_succeeds(self, urlopen, sleep):
        response = MagicMock()
        response.__enter__.return_value.status = 200
        urlopen.side_effect = [urllib.error.URLError(TimeoutError("timed out")), response]

        put_batch("https://grist.example", "doc", "table", "token", {}, retry_delay=0)

        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0)

    @patch("grist_sync.time.sleep")
    @patch("grist_sync.urllib.request.urlopen")
    def test_does_not_retry_authentication_errors(self, urlopen, sleep):
        urlopen.side_effect = urllib.error.HTTPError(
            "https://grist.example", 401, "Unauthorized", {}, io.BytesIO(b"unauthorized")
        )

        with self.assertRaises(urllib.error.HTTPError):
            put_batch("https://grist.example", "doc", "table", "token", {}, retry_delay=0)

        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
