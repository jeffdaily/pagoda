/** Perform various block-sized pnetcdf reads. */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <mpi.h>
#include <ga.h>
#include <macdecls.h>
#include <pnetcdf.h>

#include <iostream>

#include "Debug.H"

using std::cerr;
using std::cout;
using std::endl;

#define ERR(e) do { \
    if (NC_NOERR != e) { \
        cerr << ncmpi_strerror(e) << endl; \
    } \
} while (0)


void trace(int ndim, MPI_Offset *start, MPI_Offset *count);
void trace(int ndim, int64_t *lo, int64_t *hi);
void trace(int ndim, int64_t *ld);


/**
 * 1) Open a netcdf file.
 * 2) Get dimensions of given variable.
 * 3) Attempt to read into a GA with default chunking.
 * 4) Attempt to read into a GA with no chunking on last dimension.
 */
int main(int argc, char **argv)
{
    int ncid;
    int varid;
    int ndim;
    int unlimdim;
    int *dimids;
    MPI_Offset *dimlens;
    size_t dimidx;
    int g_a;
    int ga_ndim;
    nc_type type;
    int ga_type;
    int64_t *shape;
    int64_t *chunk;
    int64_t *lo;
    int64_t *hi;
    int64_t *ld;
    MPI_Offset *start;
    MPI_Offset *count;

    MPI_Init(&argc, &argv);
    GA_Initialize();

    if (3 != argc) {
        if (0 == GA_Nodeid()) {
            cerr << "Usage: TestRead <filename> <variablename>" << endl;
            for (int i=0; i<argc; ++i) {
                cerr << "argv[" << i << "]=" << argv[i] << endl;
            }
        }
        return 1;
    }

    ERR(ncmpi_open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &ncid));
    ERR(ncmpi_inq_unlimdim(ncid, &unlimdim));
    ERR(ncmpi_inq_varid(ncid, argv[2], &varid));

    ERR(ncmpi_inq_varndims(ncid, varid, &ndim));
    dimids = new int[ndim];
    dimlens = new MPI_Offset[ndim];
    start = new MPI_Offset[ndim];
    count = new MPI_Offset[ndim];

    ERR(ncmpi_inq_vardimid(ncid, varid, dimids));
    // If it's a record variable, have GA ignore the record dimension.
    if (unlimdim == dimids[0]) {
        ga_ndim = ndim - 1;
    } else {
        ga_ndim = ndim;
    }
    shape = new int64_t[ga_ndim];
    chunk = new int64_t[ga_ndim];
    lo = new int64_t[ga_ndim];
    hi = new int64_t[ga_ndim];
    if (1 < ga_ndim) {
        ld = new int64_t[ga_ndim-1];
    }
    for (int i=0; i<ndim; ++i) {
        ERR(ncmpi_inq_dimlen(ncid, dimids[i], &dimlens[i]));
    }
    // If it's a record variable, have GA ignore the record dimension.
    if (unlimdim == dimids[0]) {
        for (int i=0; i<ga_ndim; ++i) {
            shape[i] = dimlens[i+1];
            chunk[i] = 0;
        }
    } else {
        for (int i=0; i<ga_ndim; ++i) {
            shape[i] = dimlens[i];
            chunk[i] = 0;
        }
    }
    // Don't chunk last dimension.
    chunk[ga_ndim-1] = shape[ga_ndim-1];

    ERR(ncmpi_inq_vartype(ncid, varid, &type));
    if (NC_INT == type) {
        ga_type = C_INT;
    } else if (NC_FLOAT == type) {
        ga_type = C_FLOAT;
    } else if (NC_DOUBLE == type) {
        ga_type = C_DBL;
    }

    // Create a GA with default chunking.
    g_a = NGA_Create64(ga_type, ga_ndim, shape, "default chunking", chunk);
    if (ga_type == C_INT) {
        int ONE = 1;
        GA_Fill(g_a, &ONE);
    } else if (ga_type == C_FLOAT) {
        float ONE = 1.0f;
        GA_Fill(g_a, &ONE);
    } else if (ga_type == C_DBL) {
        double ONE = 1.0;
        GA_Fill(g_a, &ONE);
    }
    NGA_Distribution64(g_a, GA_Nodeid(), lo, hi);
    trace(ga_ndim, lo, hi);

    // Now for the read into the GA.
    if (0 > lo[0] && 0 > hi[0]) {
        // make a non-participating process a no-op
        for (dimidx=0; dimidx<ndim; ++dimidx) {
            start[dimidx] = 0;
            count[dimidx] = 0;
        }
        trace(ndim, start, count);
#define read_var_all(TYPE, NC_TYPE) \
        if (type == NC_TYPE) { \
            ERR(ncmpi_get_vara_##TYPE##_all(ncid, varid, start, count, NULL)); \
            TRACER("no-op\n"); \
        } else
        read_var_all(int,    NC_INT)
        read_var_all(float,  NC_FLOAT)
        read_var_all(double, NC_DOUBLE)
        ; // for last else above
#undef read_var_all
    } else {
        if (unlimdim == dimids[0] && ndim > 1) {
            start[0] = 0;
            count[0] = 1;
            for (dimidx=1; dimidx<ndim; ++dimidx) {
                start[dimidx] = lo[dimidx-1];
                count[dimidx] = hi[dimidx-1] - lo[dimidx-1] + 1;
            }
        } else {
            for (dimidx=0; dimidx<ndim; ++dimidx) {
                start[dimidx] = lo[dimidx];
                count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
            }
        }
        trace(ndim, start, count);
#define read_var_all(TYPE, NC_TYPE, FMT) \
        if (type == NC_TYPE) { \
            TYPE *ptr; \
            NGA_Access64(g_a, lo, hi, &ptr, ld); \
            ERR(ncmpi_get_vara_##TYPE##_all(ncid, varid, start, count, ptr)); \
            TRACER("ptr[0]="#FMT"\n", ptr[0]); \
        } else
        read_var_all(int, NC_INT, %d)
        read_var_all(float, NC_FLOAT, %f)
        read_var_all(double, NC_DOUBLE, %f)
        ; // for last else above
#undef read_var_all
        NGA_Release_update64(g_a, lo, hi);
    }
    GA_Print(g_a);

    ERR(ncmpi_close(ncid));

    delete [] dimids;
    delete [] dimlens;
    delete [] shape;
    delete [] lo;
    delete [] hi;
    delete [] ld;
    delete [] start;
    delete [] count;

    GA_Terminate();
    MPI_Finalize();

    return 0;
}


void trace(int ndim, MPI_Offset *start, MPI_Offset *count)
{
#ifdef TRACE
    if (ndim == 1) {
        PRINT_SYNC("start={%ld}\tcount={%ld}\n",
                start[0], count[0]);
    } else if (ndim == 2) {
        PRINT_SYNC("start={%ld,%ld}\tcount={%ld,%ld}\n",
                start[0], start[1], count[0], count[1]);
    } else if (ndim == 3) {
        PRINT_SYNC("start={%ld,%ld,%ld}\tcount={%ld,%ld,%ld}\n",
                start[0], start[1], start[2],
                count[0], count[1], count[2]);
    } else if (ndim == 4) {
        PRINT_SYNC("start={%ld,%ld,%ld,%ld}\tcount={%ld,%ld,%ld,%ld}\n",
                start[0], start[1], start[2], start[3],
                count[0], count[1], count[2], count[3]);
    }
#endif
}


void trace(int ndim, int64_t *lo, int64_t *hi)
{
#ifdef TRACE
    if (ndim == 1) {
        PRINT_SYNC("lo={%ld}\thi={%ld}\n",
                lo[0], hi[0]);
    } else if (ndim == 2) {
        PRINT_SYNC("lo={%ld,%ld}\thi={%ld,%ld}\n",
                lo[0], lo[1], hi[0], hi[1]);
    } else if (ndim == 3) {
        PRINT_SYNC("lo={%ld,%ld,%ld}\thi={%ld,%ld,%ld}\n",
                lo[0], lo[1], lo[2], hi[0], hi[1], hi[2]);
    } else if (ndim == 4) {
        PRINT_SYNC("lo={%ld,%ld,%ld,%ld}\thi={%ld,%ld,%ld,%ld}\n",
                lo[0], lo[1], lo[2], lo[3], hi[0], hi[1], hi[2], hi[3]);
    }
#endif
}


void trace(int ndim, int64_t *ld)
{
#ifdef TRACE
    if (ndim == 1) {
        PRINT_SYNC("ld={%ld}\n", ld[0]);
    } else if (ndim == 2) {
        PRINT_SYNC("ld={%ld,%ld}\n", ld[0], ld[1]);
    } else if (ndim == 3) {
        PRINT_SYNC("ld={%ld,%ld,%ld}\n", ld[0], ld[1], ld[2]);
    }
#endif
}
