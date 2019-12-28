from unittest import TestCase

from diycrate.cache_utils import redis_key


class CacheUtilTests(TestCase):
    def test_redis_key(self):
        self.assertTrue(redis_key("hello").startswith("diy_crate.version."))
