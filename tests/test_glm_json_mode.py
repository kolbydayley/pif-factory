import io
import json
import unittest
from unittest.mock import patch

from research_factory.cheap_lane_adapters import draft_glm_http


class GLMJSONModeTests(unittest.TestCase):
    def test_json_mode_is_explicit_and_default_transport_is_unchanged(self):
        for enabled in (False, True):
            captured = []

            def respond(request, **kwargs):
                captured.append(json.loads(request.data))
                return io.BytesIO(json.dumps({"choices": [{"message": {
                    "content": '{"synthetic": true}'}}]}).encode())

            with patch("pathlib.Path.read_text", return_value=json.dumps({
                    "zai-coding-plan": {"key": "synthetic-test-not-a-credential"}})), \
                    patch("urllib.request.urlopen", side_effect=respond):
                result = draft_glm_http("Return a JSON object.", json_mode=enabled)
            self.assertTrue(result["ok"])
            self.assertEqual(result["calls"], 1)
            self.assertEqual(captured[0]["thinking"], {"type": "disabled"})
            if enabled:
                self.assertEqual(captured[0]["response_format"], {"type": "json_object"})
            else:
                self.assertNotIn("response_format", captured[0])


if __name__ == "__main__":
    unittest.main()
