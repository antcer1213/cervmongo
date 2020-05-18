cervmongo package
=================

package contents
----------------

.. automodule:: cervmongo
   :members:
   :inherited-members:
   :imported-members:
   :undoc-members:
   :show-inheritance:


cervmongo.config submodule
--------------------------

.. automodule:: cervmongo.config
   :members:
   :undoc-members:
   :show-inheritance:

cervmongo.main submodule
------------------------

.. automodule:: cervmongo.main
   :members:
   :undoc-members:
   :show-inheritance:

.. py:class:: cervmongo.main.SyncIOClient.COLLECTION.CollectionClient

   Convenience class that is instantiated and supplied by the `COLLECTION <./cervmongo.html#cervmongo.main.SyncIOClient.COLLECTION>`__ method. It auto-supplies collection to all upper-cased SyncIOClient methods, where required |main-sourcelink|

   .. |main-sourcelink| raw:: html

      <a href="./_modules/cervmongo/main.html#SyncIOClient.COLLECTION">
      <span class="viewcode-link">[source]</span>
      </a>

   .. py:method:: get_client()
   .. py:method:: GENERATE_ID(_id=None)
   .. py:method:: COLLECTION(collection)
   .. py:method:: UPLOAD(fileobj, filename=None, content_type=None, extension=None, **kwargs)
   .. py:method:: DOWNLOAD(filename_or_id=None, revision=- 1, skip=None, limit=None, sort=- 1, **query)
   .. py:method:: ERASE(filename_or_id, revision=- 1)
   .. py:method:: INDEX(key='_id', sort=1, unique=False, reindex=False)
   .. py:method:: ADD_FIELD(field, value='', data=False, query={})
   .. py:method:: REMOVE_FIELD(field, query={})
   .. py:method:: DELETE(record_or_records, soft=False)
   .. py:method:: GET(id_or_query={}, sort=1, key='_id', count=None, search=None, fields=None, page=None, perpage=False, limit=None, after=None, before=None, empty=None, distinct=None, one=False, **kwargs)
   .. py:method:: POST(record_or_records)
   .. py:method:: PUT(record_or_records)
   .. py:method:: PATCH(original, updates, w=1, upsert=False, multi=False, log=None)
   .. py:method:: REPLACE(original, replacement, upsert=False)
   .. py:method:: SEARCH(search, **kwargs)
   .. py:method:: PAGINATED_QUERY(limit=20, sort=<Pagination Sort Fields._id: '_id'>, after=None, before=None, page=None, endpoint='/', ordering=-1, query={}, **kwargs)

cervmongo.aio submodule
-----------------------

.. automodule:: cervmongo.aio
   :members:
   :undoc-members:
   :show-inheritance:

.. py:class:: cervmongo.aio.AsyncIOClient.COLLECTION.CollectionClient

   Convenience class that is instantiated and supplied by the `COLLECTION <./cervmongo.html#cervmongo.aio.AsyncIOClient.COLLECTION>`__ method. It auto-supplies collection to all upper-cased AsyncIOClient methods, where required |aio-sourcelink|

   .. |aio-sourcelink| raw:: html

      <a href="./_modules/cervmongo/aio.html#AsyncIOClient.COLLECTION">
      <span class="viewcode-link">[source]</span>
      </a>

   .. py:method:: get_client()
   .. py:method:: GENERATE_ID(_id=None)
   .. py:method:: COLLECTION(collection)
   .. py:method:: UPLOAD(fileobj, filename=None, content_type=None, extension=None, **kwargs)
      :async:
   .. py:method:: DOWNLOAD(filename_or_id=None, revision=- 1, skip=None, limit=None, sort=- 1, **query)
      :async:
   .. py:method:: ERASE(filename_or_id, revision=- 1)
      :async:
   .. py:method:: INDEX(key='_id', sort=1, unique=False, reindex=False)
   .. py:method:: ADD_FIELD(field, value='', data=False, query={})
      :async:
   .. py:method:: REMOVE_FIELD(field, query={})
      :async:
   .. py:method:: DELETE(record_or_records, soft=False)
      :async:
   .. py:method:: GET(id_or_query={}, sort=1, key='_id', count=None, search=None, fields=None, page=None, perpage=False, limit=None, after=None, before=None, empty=None, distinct=None, one=False, **kwargs)
      :async:
   .. py:method:: POST(record_or_records)
      :async:
   .. py:method:: PUT(record_or_records)
      :async:
   .. py:method:: PATCH(original, updates, w=1, upsert=False, multi=False, log=None)
      :async:
   .. py:method:: REPLACE(original, replacement, upsert=False)
      :async:
   .. py:method:: SEARCH(search, **kwargs)
      :async:
   .. py:method:: PAGINATED_QUERY(limit=20, sort=<Pagination Sort Fields._id: '_id'>, after=None, before=None, page=None, endpoint='/', ordering=-1, query={}, **kwargs)
      :async:

cervmongo.models submodule
--------------------------

.. automodule:: cervmongo.models
   :members:
   :undoc-members:
   :show-inheritance:

cervmongo.utils submodule
-------------------------

.. automodule:: cervmongo.utils
   :members:
   :undoc-members:
   :show-inheritance:

cervmongo.vars submodule
------------------------

.. automodule:: cervmongo.vars
   :members:
   :undoc-members:
   :show-inheritance:


Subpackages
-----------

.. toctree::
   :maxdepth: 4

   cervmongo.extra
