# -*- coding: utf-8 -*-
import datetime
import random
import time
import unittest

from gae_datastorecache.models import DatastoreCache as Cache
from gae_datastorecache.models import DatastoreCacheItem as Item
from google.appengine.ext import db


class DatastoreCacheParseKeyTests(unittest.TestCase):

    def test_parse_key_with_empty_string(self):
        self.assertEquals(Cache._parse_key(''), '')

    def test_parse_key(self):
        self.assertEquals(Cache._parse_key('whatever'), 'whatever')

    def test_parse_key_from_tuple(self):
        self.assertEquals(Cache._parse_key(('what', 'ever')), 'ever')

    def test_parse_key_from_long_tuple(self):
        self.assertEquals(Cache._parse_key(('w', 'h', 'a', 't')), 'h')

    def test_parse_key_from_too_short_tuple(self):
        self.assertRaises(TypeError, Cache._parse_key, ('whatever', ))

    def test_parse_key_from_long_list(self):
        self.assertRaises(TypeError, Cache._parse_key, ['w', 'h', 'a', 't'])


class DatastoreCacheParseTimeTests(unittest.TestCase):

    def test_parse_time_with_zero(self):
        self.assertEquals(Cache._parse_time(0), datetime.datetime(
            year=datetime.MAXYEAR, month=12, day=31))

    def test_parse_time_with_string(self):
        self.assertRaises(TypeError, Cache._parse_time, '1033523')

    def test_parse_time_with_14_days_in_seconds(self):
        seconds = 14 * 24 * 60 * 60
        now = datetime.datetime.now()
        then = now + datetime.timedelta(seconds=seconds)

        expire_at = Cache._parse_time(seconds)
        self.assertTrue((expire_at - then).seconds <= 5)

    def test_parse_time_with_32_days_in_seconds(self):
        # Only 31 days in seconds are appropriate, otherwise it gets handled
        # as a unix timestamp
        seconds = 32 * 24 * 60 * 60
        self.assertEquals(Cache._parse_time(seconds),
            datetime.datetime.fromtimestamp(seconds))


class DatastoreCacheGetKeyNameTests(unittest.TestCase):

    def test_short_without_namespace(self):
        key = 'whatever'
        self.assertEquals(Cache._get_key_name(key), key)

    def test_long_without_namespace(self):
        key = 'a' * 300
        key_name = Cache._get_key_name(key)
        hash_length = 64

        self.assertNotEquals(key_name, key)
        self.assertEquals(len(key_name), hash_length)

    def test_short_with_namespace(self):
        key = 'whatever'
        namespace = 'prefix'

        key_name = Cache._get_key_name(key, namespace=namespace)
        self.assertEquals(key_name, ''.join([namespace, key]))

    def test_long_with_namespace(self):
        key = 'a' * 300
        namespace = 'prefix'
        hash_length = 64

        key_name = Cache._get_key_name(key, namespace=namespace)
        self.assertNotEquals(key_name, ''.join([namespace, key]))
        self.assertTrue(key_name.startswith(namespace))
        self.assertEquals(len(key_name), sum([len(namespace), hash_length]))


class DatastoreCachePublicMethodsTests(unittest.TestCase):

    def setUp(self):
        self.key = 'mykey'
        self.namespace = 'prefix'
        self.value = 'what about german characters like ä ü ö and ß?'

    def raise_exception(self):
        raise Exception()

    def test_set_and_get_with_expiration(self):
        self.assertTrue(Cache.set(self.key, self.value, time=1,
            namespace=self.namespace))
        cached_value = Cache.get(self.key, namespace=self.namespace)
        self.assertEquals(self.value, cached_value)
        time.sleep(1)
        self.assertFalse(Cache.get(self.key, namespace=self.namespace))

    def test_set_when_put_fails(self):
        _original_put = Item.put
        Item.put = self.raise_exception
        self.assertFalse(Cache.set(self.key, self.value))
        Item.put = _original_put

    def test_get_with_delete_expired(self):
        millenium_unixtime = 946706400

        # Set the already expired item
        self.assertTrue(Cache.set(self.key, self.value,
            time=millenium_unixtime))
        # Check if it exists in the Datastore
        self.assertTrue(Item.all().filter('cache_key =', self.key).get())
        # Try to get it should return None, but force to keep the item
        self.assertEquals(Cache.get(self.key, delete_expired=False), None)
        # Check again if it exists in the Datastore
        self.assertTrue(Item.all().filter('cache_key =', self.key).get())
        # Try to get it should return None and delete the item
        self.assertEquals(Cache.get(self.key), None)
        # Check again if it exists in the Datastore, it should have been del.
        self.assertFalse(Item.all().filter('cache_key =', self.key).get())

    def test_delete_for_non_available_key(self):
        key = 'nonexistingkey%s' % str(random.random())
        self.assertEquals(Cache.delete(key), 1)

    def test_delete_existing_key_with_network_problem(self):
        _original_delete = Item.delete
        Item.delete = self.raise_exception

        self.assertTrue(Cache.set(self.key, self.value))
        self.assertEquals(Cache.delete(self.key), 0)

        Item.delete = _original_delete

    def test_delete_existing_key(self):
        self.assertTrue(Cache.set(self.key, self.value))
        self.assertEquals(Cache.delete(self.key), 2)

    def test_add(self):
        key = 'nonexistingkey%s' % str(random.random())
        # Non existing key gets added
        self.assertTrue(Cache.add(self.key, self.value))
        # Existing key cannot get added
        self.assertFalse(Cache.add(self.key, self.value))

    def test_replace(self):
        self.assertFalse(Cache.replace(self.key, self.value))
        self.assertTrue(Cache.set(self.key, self.value))
        self.assertTrue(Cache.replace(self.key, 'new value'))

    def test_flush_all_with_less_than_1000_items(self):
        self.assertTrue(Cache.set('1', self.value))
        self.assertTrue(Cache.set('2', self.value))
        self.assertTrue(Cache.flush_all())
        self.assertFalse(Item.all().fetch(1000))

    def test_flush_all_should_fail_because_of_too_many_items(self):
        self.assertTrue(Cache.set('1', self.value))
        self.assertTrue(Cache.set('2', self.value))
        self.assertFalse(Cache.flush_all(max_items=1))

    def test_flush_all_with_invalid_max_items(self):
        self.assertRaises(ValueError, Cache.flush_all, {'max_items': 0})
        self.assertRaises(ValueError, Cache.flush_all, {'max_items': 1001})

    def test_flush_all_but_db_delete_fails(self):
        _original_delete = db.delete
        db.delete = self.raise_exception

        self.assertTrue(Cache.set('1', self.value))
        self.assertTrue(Cache.set('2', self.value))
        self.assertFalse(Cache.flush_all())

        db.delete = _original_delete
