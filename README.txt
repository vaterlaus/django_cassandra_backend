Introduction
============
This is an early development release of a Django backend for the Cassandra database.
It has only been under development for a short time and there are almost certainly
issues/bugs with this release -- see the end of this document for a list of known
issues. Needless to say, you shouldn't use this release in a production setting, the
format of the data stored in Cassandra may change in future versions, there's no
promise of backwards compatibility with this version, and so on.

Please let me know if you find any bugs or have any suggestions for how to improve
the backend. You can contact me at: rob.vaterlaus@gmail.com

Installation
============
The backend requires at least the 0.7 version of Cassandra. 0.7 has several features
(e.g. programmatic creation/deletion of keyspaces & column families, secondary index
support) that are useful for the Django database backend, so I targeted that
instead of 0.6. Unfortunately, the Cassandra Thrift API changed between 0.6 and 0.7,
so the two version are incompatible.

I currently use the 1.0.10 release. That's the only version I test against, so no
promises if you try it with a different version. I have tested earlier versions
against the 0.7.x and 0.8.x versions of Cassandra with no problem, so I would expect
that it would still work.

If you're updating from a previous version of the Cassandra DB backend, then it's
possible/likely that the format it stores models/fields in Cassandra has changed,
so you should wipe your Cassandra database. If you're using the default locations
for things, then this should involve executing something like "rm -rf /var/log/cassandra/*"
and "rm -rf /var/lib/cassandra/*". At some point as the backend becomes more stable
data format compatibility or migration will be supported, but for now it's not worth
the effort.

The backend also requires the Django-nonrel fork of Django and djangotoolbox.
Both are available here: <http://www.allbuttonspressed.com/projects/django-nonrel>.
I installed the Django-nonrel version of Django globally in site-packages and
copied djangotoolbox into the directory where I'm testing the Cassandra backend,
but there are probably other (better, e.g. virtualenv) ways to install those things.
I'm using the current (as of 11/1/2011) version of both packages. The Django-nonrel is
based on the 1.3 beta 1 release of Django and the version of djangotoolbox is 0.9.2.

You also need to generate the Python Thrift API code as described in the Cassandra
documentation and copy the generated "cassandra" directory (from Cassandra's
interface/gen-py directory) over to the top-level Django project directory.
You should use the 0.6.x version of Thrift if you're using the 0.8 or higher version
of Cassandra. You should use the 0.5.x version of Thrift if you're using 0.7.

To configure a project to use the Cassandra backend all you have to do is change
the database settings in the settings.py file. Change the ENGINE value to be
'django_cassandra.db' and the NAME value to be the name of the keyspace to use.
You also need to set the SUPPORTS_TRANSACTIONS setting to False, since Cassandra
doesn't support transactions. You can set HOST and PORT to specify the host and
port where the Cassandra daemon process is running. If these aren't specified
then the backend uses default values of 'localhost' and 9160.  You can also set
USER and PASSWORD if you're using authentication with Cassandra. You can also set
a few optional Cassandra-specific settings in the database settings. Set the
CASSANDRA_REPLICATION_FACTOR and CASSANDRA_STRATEGY_CLASS settings to be the
replication factor and strategy class value you want to use when the backend
creates the keyspace during syncdb. The default values for these settings are
1 and "org.apache.cassandra.locator.SimpleStrategy". You can also define
CASSANDRA_READ_CONSISTENCY_LEVEL and CASSANDRA_WRITE_CONSISTENCY_LEVEL to be
the values you want to use for the consistency level for read and write
operations. If you want to use different consistency level values for
different operations or different column families then it should work to
use the Django multiple database support to define different database
settings with different consistency levels and use the appropriate one,
but I haven't tested this to verify that it works.

Configure Cassandra as described in the Cassandra documentation.
If want to be able to do range queries over primary keys then you need to set the
partitioner in the cassandra.yaml config file to be the OrderPreservingPartitioner.

Once you're finished configuring Cassandra start up the Cassandra daemon process as
described in the Cassandra documentation.

Run syncdb. This creates the keyspace (if necessary) and the column families for the
models in the installed apps. The Cassandra backend creates one column family per
model. It uses the db_table value from the meta settings for the name of the
column family if it's specified; otherwise it uses the default name similar to
other backends.

Now you should be able to use the normal model and query set calls from your
Django code.

The backend supports query set update operations. This doesn't have the same
transactional semantics that it would have on a relational database, but it
does mean that you can use the backend with code that depends on this feature.
In particular it means that cascading deletes are now supported. For large data
sets cascading deletes are typically a bad idea, so they are disabled by default.
To enable them you define a value in the database settings dictionary named
"CASSANDRA_ENABLE_CASCADING_DELETES" whose value is True.

The backend supports automatic construction of compound id/pk fields that
are composed of the values of other fields in the model. You would typically
use this when you have some subset of the fields in the model that together
uniquely identify that particular instance of the model. Compound key generation
is enabled for a model by defining a class variable named COMPOUND_KEY_FIELDS
in a nested class called "CassandraSettings" of the model. The value of the
COMPOUND_KEY_FIELDS value is a tuple of the names of the fields that are used
to form the compound key. By default the field values are separated by the '|'
character, but this separator value can be overridden by defining a class
variable in the CassandraSettings named COMPOUND_KEY_SEPARATOR whose value is
the character to use as the separator.

This release includes a test project and app. If you want to use the backend in
another project you can copy the django_cassandra directory to the 
top-level directory of the project (along with the cassandra and djangotoolbox
directories) or else make sure that these are installed in your environment.

What Works
==========
- the basics: creating model instances, querying (get/filter/exclude), count,
  update/save, delete, order_by
- efficient queries for exact matches on the primary key. It can also do range
  queries on the primary key, but your Cassandra cluster must be configured to use the
  OrderPreservingPartitioner if you want to do that. Unfortunately, currently it 
  doesn't fail gracefully if you try to do range queries when using the
  RandomPartitioner, so just don't do that :-)
- inefficient queries for everything else that can't be done efficiently in
  Cassandra. The basic approach used in the query processing code is to first try
  to prune the number of rows to look at by finding a part of the query that can
  be evaluated efficiently (i.e. a primary key filter predicate or an exact match
  secondary index predicate). Then it evaluates the remaining filter
  predicates over the pruned rows to obtain the final result. If there's no part
  of the query that can be evaluated efficiently, then it just fetches the entire
  set of rows and does all of the filtering in the backend code.
- programmatic creation of the keyspace & column families via syncdb
- Django admin UI, except for users in the auth application (see below)
- I think all of the filter operations (e.g. gt, startswith, regex, etc.) are supported
  although it's possible I missed something
- complex queries with Q nodes
- basic secondary index support. If the db_index attribute of a field is set to True,
  then the backend configures the column family to index on that field/column.
  Currently Cassandra only supports exact match queries with the secondary
  indexes, so the support is limited. Range queries on columns with secondary indexes
  will still be inefficient.
- support for query update operations (and thus cascading deletes, but that's
  disabled by default)
  
What Doesn't Work (Yet)
=======================
- I haven't tested all of the different field types, so there are probably
  issues there with how the data is converted to and from Cassandra with some of the
  field types. My use case was mostly string fields, so most of the testing was with
  that. I've also tried out integer, float, boolean, date, datetime, time, text
  and decimal fields, so I think those should work too, but I haven't tested all
  of the possible field types.
- joins
- chunked queries. It just tries to get everything all at once from Cassandra.
  Currently the maximum number of keys/rows that it can fetch (i.e. the count
  value in the Cassandra Thrift API) defaults semi-arbitrarily to 1000000, so
  if you try to query over a column family with more returned rows than that
  it won't work (and if you're anywhere near approaching that limit you're going
  to be using gobs of memory). Similarly, there's a limit of 10000 for the number
  of columns returned in a given row. It's doubtful that anyone would come
  anywhere near that limit, since that is dictated by the number of fields there
  are in the Django model. You override either/both of these limits by setting
  the CASSANDRA_MAX_KEY_COUNT and/or CASSANDRA_MAX_COLUMN_COUNT settings in the
  database settings in settings.py.
- ListModel/ListField support from djangotoolbox (I think?). I haven't
  investigated how this works and if it's feasible to support in Cassandra,
  although I'm guessing it probably wouldn't be too hard. For now, this means
  that several of the unit tests from djangotoolbox fail if you have that
  in your installed apps. I made a preliminary pass to try to get this to
  work, but it turned out to be more difficult than expected, so it exists
  in a partially-completed form in the source.
- probably a lot of other stuff that I've forgotten or am unaware of :-)
  
Known Issues
============
- I haven't been able to get the admin UI to work for users in the Django
  authentication middleware. I included djangotoolbox in my installed apps, as
  suggested on the Django-nonrel web site, which got my further, but I still get
  an error in some Django template code that tries to render a change list (I think).
  I still need to track down what's going on there.
- There's a reported issues with using unicode strings. At this point it's
  still unclear whether this is a problem in the Django backend or in the
  Python Thrift bindings to Cassandra. I've think I've fixed all of the obvious
  places in the backend code to deal properly with Unicode strings, but it's
  possible/probable there are some remaining issues. The reported problem is with
  using non-ASCII characters in the model definitions. This triggers an exception
  during syncdb, so for now just don't do that. It hasn't been tested yet
  whether there's a problem with simply storing Unicode strings as the field
  values (as opposed to the model/field names).
- There are a few unit tests that fail in the sites middleware. These don't fail
  with the other nonrel backends, so it's a bug/limitation in the Cassandra backend.
- If you enable the authentication and session middleware a bunch of the
  associated unit tests fail if you run all of the unit tests.
  Waldemar says that it's expected that some of these unit tests will fail,
  because they rely on joins which aren't supported yet. I haven't verified yet
  that all of the failures are because of joins, though.
- the code needs a cleanup pass for things like the exception handling/safety,
  some refactoring, more pydoc comments, etc.
- I have a feeling there are some places where I haven't completely leveraged
  the code in djangotoolbox, so there may be places where I haven't done
  things in the optimal way
- the error handling/messaging isn't great for things like the Cassandra
  daemon not running, a versioning mismatch between client and Cassandra
  daemon, etc. Currently you just get a somewhat uninformative exception in
  these cases.

Changes for 0.2.4
=================
- switch the timestamp format to use the system time in microseconds to be
  consistent with the standard Cassandra timestamps used by other Cassandra
  components (e.g. the Cassandra CLI) and to hopefully eliminate issues with
  timestamp collisions across multiple Django processes.

Changes for 0.2.3
=================
- fixed a bug with the retry/reconnect logic where it would use a stale Cassandra
  Client object.

Changes for 0.2.2
=================
- fixed a bug with handling delete operations where it would sometimes incorrectly
  delete all items whose values were a substring of the specified query value
  instead of only if there was an exact match.
  
Changes for 0.2.1
=================

- Fixed typo in the CassandraAccessError class
- Added support for customizing the arguments that are used to create the
  keyspace. In particular this allows you to specify the durable_writes
  setting that was added in Cassandra 1.0 if you want to disable that for
  a keyspace.
  
Changes for 0.2
===============
- added support for automatic construction of compound id/pk fields that
are composed of the values of other fields in the model. You would typically
use this when you have some subset of the fields in the model that together
uniquely identify that particular instance of the model. Compound key generation
is enabled for a model by defining a class variable named COMPOUND_KEY_FIELDS
in a nested class called "CassandraSettings" of the model. The value of the
COMPOUND_KEY_FIELDS value is a tuple of the names of the fields that are used
to form the compound key. By default the field values are separated by the '|'
character, but this separator value can be overridden by defining a class
variable in the CassandraSettings named COMPOUND_KEY_SEPARATOR whose value is
the character to use as the separator.
- added support for running under the 0.8 version of Cassandra. This included
fixing a bug where the secondary index names were not properly scoped with
its associated column family (which "worked" before because Cassandra wasn't
properly checking for conflicts) and properly setting the replication factor
as a strategy option instead of a field in the KsDef struct. The code checks
the API version to detect whether it's running against the 0.7 or 0.8 version
of Cassandra, so it still works under 0.7.
- support for query set update operations
- support for cascading deletes (disabled by default)
- fixed some bugs in the constructors of some exception classes
- cleaned up the code for handling reconnecting to Cassandra if there's a
disruption in the connection (e.g. Cassandra restarting).

Changes for 0.1.7
=================

- Made the max key/column counts bigger as a temporary workaround for large queries.
  Really need to support chunked operations for this to work better.

Changes for 0.1.6
=================

- Fixed a bug with handling default values of fields

Changes For 0.1.5
=================

- Fixed a bug with the Cassandra reconnection logic

Changes For 0.1.4
=================

- Fixed a bug with the id field not being properly initialized if the model
  instance is created with no intialization arguments.
- Added unit tests for the bugs that were fixed recently
- Thanks to Abd Allah Diab for reporting this bug

Changes For 0.1.3
=================

- Fixed a bug with query set filter operations if there were multiple filters
  on indexed fields (e.g. foreign key fields)
- Fixed a bug with order_by operations on foreign key fields
- Thanks to Abd Allah Diab for reporting these bugs

Changes For 0.1.2
=================

- Added support for configuring the column family definition settings so that
  you can tune the various memtable, row/key cache, & compaction settings.
  You can configure global default settings in the datbase settings in
  settings.py and you can have per-model overrides for the column family
  associated with each model. For the global settings you define an item
  in the dictionary of database settings whose key is named
  CASSANDRA_COLUMN_FAMILY_DEF_DEFAULT_SETTINGS and whose value is a dictionary
  of the optional keyword arguments to be passed to the CfDef constructor.
  Consult the Cassandra docs for the list of valid keyword args to use.
  Currently the per-model settings overrides are specified inline in the models,
  which isn't a general solution but works in most cases.
  I'm also planning on adding a way to specify these settings for models
  non-intrusively. With the current inline mechanism you define a nested class
  inside the model called 'CassandraSettings'. The column family def settings
  are specified in a class variable named COLUMN_FAMILY_DEF_SETTINGS, which
  is a dictionary of any of the optional CfDef settings that you want to
  override from the default values. All of these things are optional, so if
  you don't need to override anything you don't need to define the
  CassandraSettings class. All of the required settings for the CfDef
  (e.g. keyspace, name, etc.) are determined by other means.
- Fixed a bug in handling null/missing columns when converting from the
  value from Cassandra.
- Fixed some bugs with reconnecting to Cassandra if connectivity to
  Cassandra is disrupted.
- Added a few new tests and did some cleanup to the unit tests

Changes For 0.1.1
=================
- fixed some bugs in the cassandra reconnection logic where it was always
  retrying the operation even when it succeeded the first time.
- fixed a nasty bug with deleting instances where it would delete all
  instances whose key was a substring of the key of the instance being deleted.

