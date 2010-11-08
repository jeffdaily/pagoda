User Guide for Parallel Analysis Of Geodesic Data (Pagoda)
**********************************************************

Overview
========

Pagoda is both an API for data-parallel analysis of geodesic climate data as
well as the a set of data-parallel processing tools based on this API. The API
and the tools were designed first to support geodesic semi-structured NetCDF
data, however they are generic enough to work with regularly gridded data as
well. The command line tools are designed to be similar to the NetCDF
Operators.

Currently supported tools:

 * pgra record averages
 * pgea ensemble averages
 * pgba binary operations
 * pgsub data subsetting 

Pagoda is currently in an early release stage. While we intend to mimic the NCO
tools, some functionality is not yet available. In particular, the command line
tools do not yet process missing values. Metadata editing operations are not
implemented since they are not inherently parallel operations.

Pagoda has been tested on linux workstations and Cray Xt4 (franklin).

Motivation
==========
Analysis and visualization of high-resolution GCRM data sets requires large
amounts of memory, efficient IO, and potentially significant processing power
to perform common operations such as climatology (daily, weekly, monthly
averages), weighted averages, dimension reduction operations such as zonal
means or vertical integration as well as more sophisticated operations such as
correlation, variance, vertical integration, and standard deviations. Most
currently available tools are serial and/or operate only on regular grids.

In contrast, the GCRM generates data of higher resolution than devices can
handle or the eye can see. Community-based, parallel tools to support zooming
and roaming through this massive data are sorely needed. General purpose 3D
visualization tools provide excellent 3D visualization capabilities designed
specifically to support arbitrary unstructured grids, grid interpolation,
parallel rendering and numerous 3D visualization and analysis operations such
as zooming and slicing. However, support for typical climate analyses such as
zonal means and plots of functions of time-varying variables (e.g. timeaveraged
temperature) are not currently implemented, nor are the typical abstractions of
climate data sets provided to enable climate scientists to implement their own
analyses. Beyond the GCRM, there is a growing recognition of the need for
general parallel processing tools [Larson, DOE] for climate analysis.

Running Pagoda
==============
The following options are common to all operators:

-h, --help           print this usage information and exit

-3, --3              output file in netCDF3 CLASSIC (32-bit offset) storage
                     format

-6, --64             output file in netCDF3 64-bit offset storage format

-5, --5              output file in parallel netCDF 64-bit data format

--file_format=FMT, --fl_fmt=FMT
                     FMT=classic same as -3;
                     FMT=64bit same as -6;
                     FMT=64data same as -5;

-A, --append         append to existing output file, if any

-a, --alphabetize    disable alphabetization of extracted variables

-C, --no-coords      associated coordinate variables should not be processed

-c, --coords         all coordinate variables will be processed

-d, --dimension      dim[,min[,max[,stride]]]

--fix_rec_dmn        change record dimension into fixed dimension in output
                     file

--header_pad, --hdr_pad
                     pad output header with indicated number of bytes

-h, --history        do not append to 'history' global attribute

-o, --output         output file name (or use last positional argument)

-O, --overwrite      overwrite existing output file, if any

-p, --path           path prefix for all input filenames

-v, --variable       var1[,var2[,...]] variable(s) to process

-X, --auxiliary      lon_min,lon_max,lat_min,lat_max auxiliary coordinate
                     bounding box

-x, --exclude        extract all variables EXCEPT those specified with -v

-b, --box            north,south,east,west lat/lon bounding box

The bounding box representing the subset may be in floating point or integer
notation e.g.  "-b 20.0,-20.0,170,150" (note the lack of spaces).  NOTE: We
often test the "MJO" region which we interpret to be "-b 20,-20,170,150".

Subsetter (pgsub)
-----------------
The following options are unique to pgsub:

-j, --join           join all input files on the given dimension

-u, --union          take the union of all input files

pgsub extracts a subset of the data from one or more input files and writes the
data to an output file using the same format as the (first) input file unless
otherwise instructed.

The subsetter does not implement the data display capabilities of NCO's ncks
but does implement many of the hyperslabbing capabilities.  Hyperslabbing does
take into account the explicit topology variables and dimensions of data
represented as an unstructured grid.  Coordinate variables need not be
monotonic, either.

pgsub utilizes an aggregation capability not found in ncks.  ncks allows a
single file to be manipulated whereas pgsub will automatically perform a
"union" aggregation if more than one source file is requested on the command
line.  Alternatively, a "join existing" aggregation can be requested.  For
details on aggregations, please refer to
http://www.unidata.ucar.edu/software/netcdf/ncml/v2.2/Aggregation.html.

Binary Operator (pgbo)
----------------------
The following options are unique to pgbo:

-y, --op_typ         binary arithmetic operation: add,sbt,mlt,dvd (+,-,*,/)

NOTE: Aggregation is not supported.

pgbo performs basic arithmetic operations between the first input file and the
corresponding variables in the second input file and stores the results in an
output file.

pgbo will *broadcast* comforming variables from the second input file to those
of the first input file similar to how ncbo operates::

    Broadcasting a variable means creating data in non-existing dimensions
    from the data in existing dimensions. For example, a two dimensional
    variable in file_2 can be subtracted from a four, three, or two (but not
    one or zero) dimensional variable (of the same name) in file_1. This
    functionality allows the user to compute anomalies from the mean. Note
    that variables in file_1 are not broadcast to conform to the dimensions in
    file_2. In the future, we will broadcast variables in file_1, if necessary
    to conform to their counterparts in file_2. Thus, presently, the number of
    dimensions, or rank, of any processed variable in file_1 must be greater
    than or equal to the rank of the same variable in file_2. Furthermore, the
    size of all dimensions common to both file_1 and file_2 must be equal. 

Record Averager (pgra)
----------------------
The following options are unique to pgra:

-y, --op_typ         average operation:
                     avg,sqravg,avgsqr,max,min,rms,rmssdn,sqrt,ttl

NOTE: Aggregation is not supported.

Ensemble Averager (pgea)
------------------------
The following options are unique to pgra:

-y, --op_typ         average operation:
                     avg,sqravg,avgsqr,max,min,rms,rmssdn,sqrt,ttl

NOTE: Aggregation is not supported.
