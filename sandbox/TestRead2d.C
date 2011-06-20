/** Similar to TestRead, however this does not use pagoda. */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <ctype.h>
#include <unistd.h>

#include <mpi.h>
#include <pnetcdf.h>

#include "timer.h"

using std::accumulate;
using std::cerr;
using std::cout;
using std::endl;
using std::find;
using std::flush;
using std::multiplies;
using std::ostringstream;
using std::runtime_error;
using std::string;
using std::vector;

#define TAG_DEBUG 33284
#define STREAM stderr

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define AT string(__FILE__ ":" TOSTRING(__LINE__))

#define ERRNO(n) \
do { \
    ostringstream __os; \
    const string __where(AT); \
    const string __prefix(": Error: "); \
    const string __errstr(ncmpi_strerror(n)); \
    __os << __where << __prefix << __errstr << endl; \
    const string __msg(__os.str()); \
    throw runtime_error(__msg); \
} while (0)

#define ERRNO_CHECK(n) \
do { \
    if (NC_NOERR != n) { \
        ERRNO(n); \
    } \
} while (0)

static MPI_Offset shape_to_size(const vector<MPI_Offset> &shape)
{
    MPI_Offset result = accumulate(shape.begin(), shape.end(), 1, multiplies<MPI_Offset>());
    assert(result >= 1);
    return result;
}

#if 0
static int nc_sizeof(nc_type type) {
    int size = 1;
    switch (type) {
        case NC_BYTE: size = 1; break;
        case NC_CHAR: size = 1; break;
        case NC_SHORT: size = 2; break;
        case NC_INT: size = 4; break;
        case NC_FLOAT: size = 4; break;
        case NC_DOUBLE: size = 8; break;
        case NC_NAT: ERRNO(NC_EBADTYPE); break;
        default: ERRNO(NC_EBADTYPE); break;
    }
    assert(size >= 1);
    return size;
}
#endif

static int get_precision(MPI_Comm comm)
{
    int precision = 1;
    int npe;

    MPI_Comm_size(comm, &npe);
    while (npe > 10) {
        ++precision;
        npe /= 10;
    }
    return precision;
}

static void print_sync(const string &str, MPI_Comm comm)
{
    int me;
    int npe;

    MPI_Comm_rank(comm, &me);
    MPI_Comm_size(comm, &npe);

    if (me == 0) {
        fprintf(STREAM, "[%*d] ", get_precision(comm), 0);
        fprintf(STREAM, str.c_str());
        for (int proc=1; proc<npe; ++proc) {
            MPI_Status stat;
            int count;
            char *msg;

            MPI_Recv(&count, 1, MPI_INT, proc, TAG_DEBUG, comm, &stat);
            msg = new char[count];
            MPI_Recv(msg, count, MPI_CHAR, proc, TAG_DEBUG, comm, &stat);
            fprintf(STREAM, "[%*d] ", get_precision(comm), proc);
            fprintf(STREAM, msg);
            delete [] msg;
        }
        fflush(STREAM);
    }
    else {
        int count = str.size() + 1;

        MPI_Send(&count, 1, MPI_INT, 0, TAG_DEBUG, comm);
        MPI_Send((void*)str.c_str(), count, MPI_CHAR, 0, TAG_DEBUG, comm);
    }
}

static void print_zero(const string &str, MPI_Comm comm)
{
    int me;
    
    MPI_Comm_rank(comm, &me);
    if (0 == me) {
        fprintf(STREAM, str.c_str());
        fflush(STREAM);
    }
    MPI_Barrier(comm);
}

template <class Collection>
static string vec_to_string(
        const Collection &collection,
        char const * delimiter=",",
        const string &name="")
{
    typedef typename Collection::const_iterator iter;
    ostringstream os;
    iter beg = collection.begin();
    iter end = collection.end();

    if (!name.empty()) {
        os << name << "=";
    }

    if (beg == end) {
        return "{}";
    }

    os << "{" << *(beg++);
    for (; beg != end; ++beg) {
        os << delimiter << *beg;
    }
    os << "}";

    return os.str();
}

template <class T>
static string to_string(const T &object)
{
    ostringstream os;
    os << object;
    return os.str();
}

template <class T>
static void buf_cmp(T *buf, T *nb_buf, MPI_Offset size, const string &name)
{
    int me;
    MPI_Comm_rank(MPI_COMM_WORLD, &me);
    for (MPI_Offset i=0; i<size; ++i) {
        if (buf[i] != nb_buf[i]) {
            //if (0 == me) {
            cout << "[" << me << "] " << name << " failure: "
                << "'" << buf[i] << "'"
                << " != "
                << "'" << nb_buf[i] << "'"
                << endl;
            //}
            break;
        }
    }
}

void read(const string &filename,
        MPI_Comm comm,
        utimer_t &timer_blocking_total,
        utimer_t &timer_nonblocking_total)
{
    int me;
    int npe;
    int ncid;
    int ndims;
    int nvars;
    int natts;
    int udim;
    vector<int> dim_ids;
    vector<string> dim_names;
    vector<MPI_Offset> dim_lens;
    vector<string> var_names_user;
    vector<int> var_ids;
    vector<string> var_names;
    vector<nc_type> var_types;
    vector<int> var_ndims;
    vector<vector<int> > var_dim_ids;
    vector<vector<MPI_Offset> > var_dim_lens;
    vector<void*> buffers;
    vector<void*> nb_buffers;
    vector<MPI_Offset> buf_sizes;
    vector<int> requests;
    vector<int> statuses;
    utimer_t timer_blocking_local;
    utimer_t timer_nonblocking_local;

    timer_blocking_total=0;
    timer_nonblocking_total=0;

    MPI_Comm_rank(comm, &me);
    MPI_Comm_size(comm, &npe);

    // open the nc file, query for basic info
    ERRNO_CHECK(ncmpi_open(comm, filename.c_str(),
                NC_NOWRITE, MPI_INFO_NULL, &ncid));
    ERRNO_CHECK(ncmpi_inq(ncid, &ndims, &nvars, &natts, &udim));

    // iterate over dimensions in file, query for info
    for (int dimid=0; dimid<ndims; ++dimid) {
        string name;
        char cname[NC_MAX_NAME];
        MPI_Offset len;

        ERRNO_CHECK(ncmpi_inq_dim(ncid, dimid, cname, &len));
        name = cname;
        dim_ids.push_back(dimid);
        dim_names.push_back(name);
        dim_lens.push_back(len);
    }

    // iterate over variables in file, query for info
    for (int varid=0; varid<nvars; ++varid) {
        string name;
        char cname[NC_MAX_NAME];
        int ndimsp;
        int dimidsp[NC_MAX_VAR_DIMS];
        nc_type type;
        int nattsp;

        ERRNO_CHECK(ncmpi_inq_var(ncid, varid, cname, &type, &ndimsp,
                    dimidsp, &nattsp));
        name = cname;
        if (var_names_user.empty()
                || find(var_names_user.begin(), var_names_user.end(), name)
                != var_names_user.end()) {
            var_ids.push_back(varid);
            var_names.push_back(name);
            var_types.push_back(type);
            var_dim_ids.push_back(vector<int>(dimidsp,dimidsp+ndimsp));
            var_dim_lens.push_back(vector<MPI_Offset>(ndimsp));
            for (int i=0; i<ndimsp; ++i) {
                var_dim_lens.back()[i] = dim_lens[dimidsp[i]];
            }
        }
    }

    // for ease of implementation, every proc reads entire variable
    for (size_t i=0; i<var_ids.size(); ++i) {
        int request;
        int varid = var_ids[i];
        string name = var_names[i];
        nc_type type = var_types[i];
        vector<MPI_Offset> count = var_dim_lens[i];
        vector<MPI_Offset> start(count.size(), 0);
        MPI_Offset size;
        void *buf;
        void *nb_buf;
#define FRONT 1
#if FRONT
        if (0 == i) {
            //print_zero("decomp using first dimension\n", comm);
        }
        if (count.size() >= 1 && count[0] >= npe) {
            int piece = count[0]/npe;
            int remainder = count[0]%npe;
            if (me < remainder) {
                count[0] = piece+1;
                start[0] = piece*me+me;
            } else {
                count[0] = piece;
                start[0] = piece*me+remainder;
            }
        }
        // first dimension too small, try to distribute second dimension
        else if (count.size() >= 2 && count[1] >= npe) {
            int piece = count[1]/npe;
            int remainder = count[1]%npe;
            if (me < remainder) {
                count[1] = piece+1;
                start[1] = piece*me+me;
            } else {
                count[1] = piece;
                start[1] = piece*me+remainder;
            }
        }
#elif BACK
        if (0 == i) {
            //print_zero("decomp using last dimension", comm);
        }
        if (!count.empty() && count.back() >= npe) {
            int original = count.back();
            int piece = count.back()/npe;
            count.back() = piece;
            start.back() = piece*me;
            if (me == npe-1) {
                count.back() += original-piece*npe;
            }
        }
#endif
        size = shape_to_size(count);
        buf_sizes.push_back(size);
        //print_sync("var=" + name
        //        + "\tstart=" + vec_to_string(start)
        //        + "\tcount=" + vec_to_string(count)
        //        + "\tsize=" + to_string(size)
        //        + "\n", comm);

        // allocate buffers
        switch (type) {
            case NC_BYTE:
                buf = new signed char[size];
                nb_buf = new signed char[size];
                break;
            case NC_CHAR:
                buf = new char[size];
                nb_buf = new char[size];
                break;
            case NC_SHORT:
                buf = new short[size];
                nb_buf = new short[size];
                break;
            case NC_INT:
                buf = new int[size];
                nb_buf = new int[size];
                break;
            case NC_FLOAT:
                buf = new float[size];
                nb_buf = new float[size];
                break;
            case NC_DOUBLE:
                buf = new double[size];
                nb_buf = new double[size];
                break;
            case NC_NAT:
            default:
                ERRNO(NC_EBADTYPE);
                /*break; unreachable */
        }
        //print_zero("blocking read of " + name + " ... ", comm);
        timer_blocking_local = timer_start();
        switch (type) {
            case NC_BYTE:
                ERRNO_CHECK(ncmpi_get_vara_schar_all(ncid, varid,
                            &start[0], &count[0], (signed char *)buf));
                break;
            case NC_CHAR:
                ERRNO_CHECK(ncmpi_get_vara_text_all(ncid, varid,
                            &start[0], &count[0], (char*)buf));
                break;
            case NC_SHORT:
                ERRNO_CHECK(ncmpi_get_vara_short_all(ncid, varid,
                            &start[0], &count[0], (short*)buf));
                break;
            case NC_INT:
                ERRNO_CHECK(ncmpi_get_vara_int_all(ncid, varid,
                            &start[0], &count[0], (int*)buf));
                break;
            case NC_FLOAT:
                ERRNO_CHECK(ncmpi_get_vara_float_all(ncid, varid,
                            &start[0], &count[0], (float*)buf));
                break;
            case NC_DOUBLE:
                ERRNO_CHECK(ncmpi_get_vara_double_all(ncid, varid,
                            &start[0], &count[0], (double*)buf));
                break;
            case NC_NAT:
            default:
                ERRNO(NC_EBADTYPE);
                /*break; unreachable */
        }
        timer_blocking_total += timer_end(timer_blocking_local);
        //print_zero("done\n", comm);
        //print_zero("non-blocking read of " + name + " ... ", comm);
        timer_nonblocking_local = timer_start();
        switch (type) {
            case NC_BYTE:
                ERRNO_CHECK(ncmpi_iget_vara_schar(ncid, varid,
                            &start[0], &count[0], (signed char *)nb_buf, &request));
                break;
            case NC_CHAR:
                ERRNO_CHECK(ncmpi_iget_vara_text(ncid, varid,
                            &start[0], &count[0], (char*)nb_buf, &request));
                break;
            case NC_SHORT:
                ERRNO_CHECK(ncmpi_iget_vara_short(ncid, varid,
                            &start[0], &count[0], (short*)nb_buf, &request));
                break;
            case NC_INT:
                ERRNO_CHECK(ncmpi_iget_vara_int(ncid, varid,
                            &start[0], &count[0], (int*)nb_buf, &request));
                break;
            case NC_FLOAT:
                ERRNO_CHECK(ncmpi_iget_vara_float(ncid, varid,
                            &start[0], &count[0], (float*)nb_buf, &request));
                break;
            case NC_DOUBLE:
                ERRNO_CHECK(ncmpi_iget_vara_double(ncid, varid,
                            &start[0], &count[0], (double*)nb_buf, &request));
                break;
            case NC_NAT:
            default:
                ERRNO(NC_EBADTYPE);
                /*break; unreachable */
        }
        timer_nonblocking_total += timer_end(timer_nonblocking_local);
        //print_zero("done\n", comm);
        buffers.push_back(buf);
        nb_buffers.push_back(nb_buf);
        requests.push_back(request);
    }
    statuses.assign(requests.size(), 0);
    //print_zero("wait ... ", comm);
    timer_nonblocking_local = timer_start();
    ERRNO_CHECK(ncmpi_wait_all(
                ncid, requests.size(), &requests[0], &statuses[0]));
    timer_nonblocking_total += timer_end(timer_nonblocking_local);
    //print_zero("done\n", comm);

    // compare the buffers
    for (size_t i=0; i<buffers.size(); ++i) {
        string name = var_names[i];
        nc_type type = var_types[i];
        vector<MPI_Offset> shape = var_dim_lens[i];
        void *buf = buffers[i];
        void *nb_buf = nb_buffers[i];
        MPI_Offset size = buf_sizes[i];
        switch (type) {
            case NC_BYTE:
                buf_cmp((signed char*)buf, (signed char*)nb_buf, size, name);
                break;
            case NC_CHAR:
                buf_cmp((char*)buf, (char*)nb_buf, size, name);
                break;
            case NC_SHORT:
                buf_cmp((short*)buf, (short*)nb_buf, size, name);
                break;
            case NC_INT:
                buf_cmp((int*)buf, (int*)nb_buf, size, name);
                break;
            case NC_FLOAT:
                buf_cmp((float*)buf, (float*)nb_buf, size, name);
                break;
            case NC_DOUBLE:
                buf_cmp((double*)buf, (double*)nb_buf, size, name);
                break;
            case NC_NAT:
            default:
                ERRNO(NC_EBADTYPE);
                /*break; unreachable */
        }
    }

    // print timing info
    print_sync("   timer_blocking_total="
            + to_string(timer_blocking_total) + "\n", MPI_COMM_WORLD);
    print_sync("timer_nonblocking_total="
            + to_string(timer_nonblocking_total) + "\n", MPI_COMM_WORLD);

    // clean up
    for (size_t i=0; i<var_types.size(); ++i) {
        switch (var_types[i]) {
            case NC_BYTE:
                delete [] ((signed char*)buffers[i]);
                delete [] ((signed char*)nb_buffers[i]);
                break;
            case NC_CHAR:
                delete [] ((char*)buffers[i]);
                delete [] ((char*)nb_buffers[i]);
                break;
            case NC_SHORT:
                delete [] ((short*)buffers[i]);
                delete [] ((short*)nb_buffers[i]);
                break;
            case NC_INT:
                delete [] ((int*)buffers[i]);
                delete [] ((int*)nb_buffers[i]);
                break;
            case NC_FLOAT:
                delete [] ((float*)buffers[i]);
                delete [] ((float*)nb_buffers[i]);
                break;
            case NC_DOUBLE:
                delete [] ((double*)buffers[i]);
                delete [] ((double*)nb_buffers[i]);
                break;
            case NC_NAT:
            default:
                ERRNO(NC_EBADTYPE);
                /*break; unreachable */
        }
    }

    ERRNO_CHECK(ncmpi_close(ncid));
}

int main(int argc, char **argv)
{
    int me;
    int npe;
    int groupid;
    MPI_Comm comm;
    utimer_t timer_blocking_total=0;
    utimer_t timer_blocking_total_all=0;
    utimer_t timer_nonblocking_total=0;
    utimer_t timer_nonblocking_total_all=0;
    bool use_groups = false;
    vector<string> filenames;
    int index;
    int c;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &me);
    MPI_Comm_size(MPI_COMM_WORLD, &npe);

    opterr = 0;

    while ((c = getopt(argc, argv, "g")) != -1) {
        switch (c)
        {
            case 'g':
                use_groups = true;
                break;
            case '?':
            default:
                print_zero("bad argument\n", MPI_COMM_WORLD);
                MPI_Finalize();
                return EXIT_FAILURE;
        }
    }

    if (optind == argc) {
        print_zero("Usage: TestRead2d [-g] filename1 ...\n", MPI_COMM_WORLD);
        MPI_Finalize();
        return EXIT_FAILURE;
    }
    for (index=optind; index<argc; ++index) {
        filenames.push_back(argv[index]);
    }

    if (use_groups) {
        if (npe < filenames.size()) {
            print_zero("Too few processors\n", MPI_COMM_WORLD);
            MPI_Finalize();
            return EXIT_FAILURE;
        }
        groupid = me*filenames.size()/npe;
        if (MPI_SUCCESS != MPI_Comm_split(MPI_COMM_WORLD, groupid, 0, &comm)) {
            if (me == 0) {
                printf("uh oh\n");
            }
            MPI_Finalize();
            return EXIT_FAILURE;
        }
        print_zero("Reading files using groups!\n", MPI_COMM_WORLD);
        print_sync("Reading " + filenames[groupid] + "\n", MPI_COMM_WORLD);
        read(filenames[groupid], comm,
                timer_blocking_total, timer_nonblocking_total);
        MPI_Reduce(&timer_blocking_total, &timer_blocking_total_all, 1,
                MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&timer_nonblocking_total, &timer_nonblocking_total_all, 1,
                MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    } else {
        print_zero("Reading files without groups!\n", MPI_COMM_WORLD);
        for (size_t i=0; i<filenames.size(); ++i) {
            print_zero("Reading " + filenames[i] + "\n", MPI_COMM_WORLD);
            read(filenames[i], MPI_COMM_WORLD,
                    timer_blocking_total, timer_nonblocking_total);
            timer_blocking_total_all += timer_blocking_total;
            timer_nonblocking_total_all += timer_nonblocking_total;
        }
    }

    print_zero("   timer_blocking_total_all="
            + to_string(timer_blocking_total_all) + "\n", MPI_COMM_WORLD);
    print_zero("timer_nonblocking_total_all="
            + to_string(timer_nonblocking_total_all) + "\n", MPI_COMM_WORLD);

    MPI_Finalize();
    return EXIT_SUCCESS;
}
