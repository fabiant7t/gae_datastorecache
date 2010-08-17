import datetime
import hashlib
import pickle
import time

from google.appengine.ext import db


class DatastoreCacheItem(db.Model):
    """
    The DatastoreCacheItem persists the keys and values.
    """
    cache_key = db.StringProperty()
    pickled_value = db.BlobProperty()
    expire_at = db.DateTimeProperty()


class DatastoreCache(object):
    """
    The API to use a datastore backed cache.
    """

    @classmethod
    def _parse_key(self, key):
        """
        Parses and returns the key as a string.
        The key can be a string or a tuple of (hash_value, string)
        """
        if isinstance(key, tuple) and len(key) >= 2:
            key = key[1]
        if isinstance(key, (str, unicode, )):
            return key
        raise TypeError('Key must be a string or a tuple with the key as '\
            'second item')

    @classmethod
    def _parse_time(self, time):
        """
        Parses and returns the time as a datatime.datetime instance.
        Time must be either relative number of seconds from current time (up
        to 1 month), or an absolute Unix epoch time.
        """
        if not isinstance(time, (int, long, float)):
            raise TypeError('Time must either be a relative number of '\
                'seconds from current time (up to 1 month), or an absolute '\
                'Unix epoch time')
        if time == 0:
            dt = datetime.datetime(year=datetime.MAXYEAR, month=12, day=31)
        elif time <= 2678400: # 31 days in seconds
            dt = datetime.datetime.now() + datetime.timedelta(seconds=time)
        else:
            dt = datetime.datetime.fromtimestamp(time)

        return dt

    @classmethod
    def _get_key_name(self, key, namespace=None):
        """
        Returns the key_name including the namespace.
        If the key is longer than 250 characters, the key gets hashed.
        """
        key = self._parse_key(key)
        key_name = ''.join([namespace or '', key])
        if len(key_name) > 250:
            m = hashlib.sha256()
            m.update(key)
            key_name = ''.join([namespace or '', m.hexdigest()])
        return key_name

    @classmethod
    def _get_item(self, key, namespace=None, delete_expired=True):
        """
        Returns the matching and not expired item or None.
        When the delete_expired parameter is set to True, matching but
        expired items get deleted transparently.
        """
        key_name = self._get_key_name(key, namespace)

        item = DatastoreCacheItem.all().filter('cache_key =',
            key_name).get()
        if item is not None:
            if item.expire_at >= datetime.datetime.now():
                return item
            elif delete_expired:
                item.delete()
        return None

    @classmethod
    def set(self, key, value, time=0, min_compress_len=0, namespace=None):
        """
        Sets a key's value, regardless of previous contents in cache.
        The return value is True if set, False on error.
        """
        key_name = self._get_key_name(key, namespace)
        time = self._parse_time(time)

        old_valid_item = self._get_item(key, namespace, delete_expired=True)
        if old_valid_item is not None:
            item = old_valid_item
        else:
            item = DatastoreCacheItem(cache_key=key_name)

        item.expire_at = time
        item.pickled_value = db.Blob(pickle.dumps(value, 1))

        try:
            item.put()
        except:
            return False
        else:
            return True

    @classmethod
    def get(self, key, namespace=None, delete_expired=True):
        """
        Returns the value of the key, if found in cache, else None.
        """
        item = self._get_item(key, namespace=namespace,
            delete_expired=delete_expired)
        if item is not None:
            return pickle.loads(item.pickled_value)

        return None

    @classmethod
    def delete(self, key, seconds=0, namespace=None):
        """
        Deletes a key from cache.

        Parameter seconds: Ignored option for compatibility.
        The return value is 0 (DELETE_NETWORK_FAILURE) on network failure,
        1 (DELETE_ITEM_MISSING) if the server tried to delete the item but
        didn't have it, and 2 (DELETE_SUCCESSFUL) if the item was actually
        deleted.
        """
        item = self._get_item(key, namespace=namespace)
        if item is not None:
            try:
                item.delete()
            except:
                return 0
            else:
                return 2

        return 1

    @classmethod
    def add(self, key, value, time=0, min_compress_len=0, namespace=None):
        """
        Sets a key's value, if and only if the item is not already in cache.
        The return value is True if added, False on error.
        """
        item = self._get_item(key, namespace=namespace)
        if item is None:
            return self.set(key, value, time=time,
                min_compress_len=min_compress_len, namespace=namespace)
        return False

    @classmethod
    def replace(self, key, value, time=0, min_compress_len=0, namespace=None):
        """
        Replaces a key's value, failing if item isn't already in cache.
        The return value is True if replaced. False on error or cache miss.
        """
        item = self._get_item(key, namespace=namespace)
        if item is not None:
            return self.set(key, value, time=time,
                min_compress_len=min_compress_len, namespace=namespace)

        return False

    @classmethod
    def flush_all(self, max_items=1000):
        """
        Deletes max 1000 items in cache.
        The return value is True on success, False on error (or if there are
        items left).
        """
        if max_items < 1 or max_items > 1000:
            raise ValueError('Parameter max_items must be between 0 and 1000')

        try:
            db.delete(DatastoreCacheItem.all().fetch(max_items))
        except:
            return False

        if DatastoreCacheItem.all().fetch(1):
            return False

        return True
