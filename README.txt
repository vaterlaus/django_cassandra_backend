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
The backend requires the 0.7 version of Cassandra. 0.7 has several features
(e.g. programmatic creation/deletion of keyspaces & column families, secondary index
support) that are useful for the Django database backend, so I targeted that
instead of 0.6. Unfortunately, the Cassandra Thrift API changed between 0.6 and 0.7,
so the two version are incompatible.

There's a beta1 version of 0.7 available at the Cassandra web site. I'm actually using a
somewhat later daily binary release dated 9/4/10. You can obtain the daily release by
following the "Latest Builds" link in the Cassandra downloads page. I had switched
to the 9/4 release, because I had read that there was an issue with the secondary
index support in the beta1 release and I was trying to get secondary index support
working in the backend. It's possible the backend will work with beta1 release,
but I haven't tested with beta1, so no promises. I also tried with a couple of
the more recent daily releases (roughly mid-September) and I was seeing problems
where the database got hosed after running the unit tests (I'm guessing from the
way the unit tests create and destroy keyspaces & column families) and it seemed
like the only way to recover was to delete the data files from the file system.
So if you see that problem try reverting to the 9/4 daily build. I haven't tried
with the most recent daily builds, so it's possible that that problem has been
fixed (assuming it was a temporary bug in Cassandra, not something bad that the
backend was doing).

The backend also requires the Django-nonrel fork of Django and djangotoolbox.
Both are available here: <http://www.allbuttonspressed.com/projects/django-nonrel>.
I installed the Django-nonrel version of Django globally in site-packages and
copied djangotoolbox into the directory where I'm testing the Cassandra backend,
but there are probably other (better?) ways to install those things.

You also need to generate the Python Thrift API code as described in the Cassandra
documentation and copy the generated "cassandra" directory (from Cassandra's
interface/gen-py directory) over to the top-level Django project directory.

To configure a project to use the Cassandra backend all you have to do is change
the database settings in the settings.py file. Change the ENGINE value to be
'django_cassandra.db' and the NAME value to be the name of the keyspace to use.
You can set HOST and PORT to override the default values of 'localhost' and 9160.
In theory you can also set USER and PASSWORD if you're using authentication with
Cassandra, but this hasn't been tested yet, so it may not work.

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

This release includes a test project and app. If you want to use the backend in
another project you just need to copy the django_cassandra directory to the 
top-level directory of the project (along with the cassandra and djangotoolbox
directories).

What Works
==========
- the basics: creating model instances, querying (get/filter/exclude), count,
  update/save, delete, order_by
- efficient queries for exact matches on the primary key. It can also do range
  queries on the primary key, but your Cassandra cluster must be configured to use the
  OrderPreservingPartitioner if you want to do that. Unfortunately, currently it 
  doesn't fail gracefully if you try to do range queries when using the
  RandomPartitioner, so just don't do that for now :-)
- inefficient queries for everything else that can't be done efficiently in
  Cassandra. The basic approach used in the query processing code is to first try
  to prune the number of rows to look at by finding a part of the query that can
  be evaluated efficiently (i.e. a primary key filter predicate or a secondary
  index predicate, once that's working). Then it evaluates the remaining filter
  predicates over the pruned rows to obtain the final result. If there's no part
  of the query that can be evaluated efficiently, then it just fetches the entire
  set of rows and does all of the filtering in the backend code.
- programmatic creation of the keyspace & column families via syncdb
- Django admin UI, except for users in the auth application (see below)
- I think all of the filter operations (e.g. gt, startswith, regex, etc.) are supported
  although it's possible I missed something
- complex queries with Q nodes

What Doesn't Work (Yet)
=======================
- Secondary Index Support: There's code in there to use secondary indexes, but
  I was seeing weird results when I tried to execute Cassandra queries using the
  secondary indexes so I disabled that code. Hopefully that's just an issue with the
  specific version of Cassandra I'm using, but I haven't tried it out with a more
  recent version to see if it's working now. If you're feeling adventurous you could
  try it out with a newer version of Cassandra and enable the secondary index code
  by setting the value of SECONDARY_INDEX_SUPPORT_ENABLED to True in predicate.py.
  You enable secondary index support for fields by setting the db_index argument to
  True when constructing the field.
- I haven't tested all of the different field types, so there are probably
  issues there with how the data is converted to and from Cassandra with some of the
  field types. My use case was mostly string fields, so most of the testing was with
  that. I've also tried out date, datetime, time, and decimal fields, so I think
  those should work too, but I haven't tried anything else.
- joins
- chunked queries. It just tries to get everything all at once from Cassandra.
  Currently the maximum that it can get (i.e. the count value in the Cassandra
  Thrift API) is set semi-arbitrarily to 10000, so if you try to query over a
  column family with more rows (or columns) than that it may not work.
  Probably the value could be set higher than that, but at some point Cassandra
  fails if it's too big (i.e. it didn't work if I set it to 0x7fffffff).
  If you want to make it bigger you can change the MAX_FETCH_COUNT variable
  in compiler.py.
- ListModel/ListField support from djangotoolbox (I think?). I haven't
  investigated how this works and if it's feasible to support in Cassandra,
  although I'm guessing it probably wouldn't be too hard. For now, this means
  that several of the unit tests from djangotoolbox fail if you have that
  in your installed apps. I made a preliminary pass to try to get this to
  work, but it turned out to be more difficult than expected, so it exists
  in a partially-completed form in the source.
- there's no way to configure the settings used to create the keyspaces
  and column families (e.g. replication strategy, replication factor) or the
  read & write consistency levels used when querying or inserting/mutating
  columns in Cassandra. My plan was to add global database settings and
  per-model Meta settings to configure those things, but I haven't gotten to
  it yet.
- Cassandra authentication. Actually this may work but I haven't tested it yet.
  There's code in there that tries to login to Cassandra if the USER and
  PASSWORD are specified in the database settings, but I've only tested with
  the AllowAllAuthenticator.
- probably a lot of other stuff that I've forgotten or am unaware of :-)
  
Known Issues
============
- I haven't been able to get the admin UI to work for users in the Django
  authentication middleware. I included djangotoolbox in my installed apps, as
  suggested on the Django-nonrel web site, which got my further, but I still get
  an error in some Django template code that tries to render a change list (I think).
  I still need to track down what's going on there.
- In some cases there's a unit test failure in the test_count test at line 269. I'm
  suspicious that this is due to the secondary index support in Cassandra, but I
  haven't investigated it enough to be sure of that yet.
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
