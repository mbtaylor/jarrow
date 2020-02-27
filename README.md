JARROW
======

Overview
--------

Jarrow is a lightweight java implementation for I/O of data stored
in formats related to [Apache Arrow](https://arrow.apache.org/).
Currently, it only has support for the Arrow-related
[Feather](https://github.com/wesm/feather) format,
but it may in future grow support for the
[Arrow IPC File](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) format or other evolutions of Feather.
Or it may not.

Comparison with the Apache Java Arrow Implementation
----------------------------------------------------

Why write this when there's already a Java implementation of
Feather I/O [provided by Apache](https://arrow.apache.org/docs/java/)?
I wanted something without all those dependencies,
and for which I had full control over the data access.
I'm using it to provide Feather table I/O handlers in
[STIL](http://www.starlink.ac.uk/stil/)/[TOPCAT](http://www.starlink.ac.uk/topcat/).

This library probably does less clever stuff
(e.g. there is no attempt at parallel I/O, which I think(?) the
Apache library does), but the library is much smaller and
has no external dependencies.

Building
--------

There's a makefile, but basically you just need to run `javac`
or `javadoc` on `*.java`.  Alternatively you can just download
the `jarrow.jar` file in the release.

Implementation Status
---------------------

Only feather files are currently supported.
All feather files can be read, but currently the following column types
are not fully supported on input:

   * **CATEGORY:**: I haven't come across any feather files with category
     column types, and it's not clear to me how to interpret the feather
     format documentation for this type, so these are not supported.
   * **UINT64:** There's no java primitive or primitive-wrapper type that can 
     represent unsigned 64-bit integers, so they are not supported.
   * **TIMESTAMP, DATE, TIME:** These values can be read, but the
     type-specific metadata/unit information is not currently available.

The `LARGE_UTF8` and `LARGE_BINARY` types defined in
https://github.com/apache/arrow/blob/master/cpp/src/arrow/ipc/feather.fbs
but _not_ in
https://github.com/wesm/feather/blob/master/cpp/src/feather/metadata.fbs
are supported.


Usage
-----

Comprehensive documentation is provided in the
[javadocs](https://mbtaylor.github.io/jarrow/javadocs/).

To read a table, you can use `FeatherTable.fromFile(File)` method;
examples in `FeatherTable.main`.

To write a table, use `FeatherTableWriter.write(OutputStream)`;
this requires you to implement some `FeatherColumnWriter` objects
in some way appropriate to the data structures in which your table
data resides; there are examples in `FeatherTableWriter.main`.


Licence
-------

Available under the LGPL.  If that's not good for you I can probably
add another licence.

History
-------

* **Version 1.0 (27 Feb 2020)**: Initial release


