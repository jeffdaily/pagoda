#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <string>

#include "Hints.H"

using std::string;

string Hints::striping_unit = "";
string Hints::cb_buffer_size = "";
string Hints::romio_cb_read = "";
string Hints::romio_ds_read = "";
string Hints::romio_no_indep_rw = "true";

MPI_Info Hints::get_info()
{
    MPI_Info info;

    MPI_Info_create(&info);
    if (!Hints::striping_unit.empty()) {
        MPI_Info_set(info, "striping_unit",
                const_cast<char*>(Hints::striping_unit.c_str()));
    }
    if (!Hints::romio_cb_read.empty()) {
        MPI_Info_set(info, "romio_cb_read",
                const_cast<char*>(Hints::romio_cb_read.c_str()));
    }
    if (!Hints::romio_ds_read.empty()) {
        MPI_Info_set(info, "romio_ds_read",
                const_cast<char*>(Hints::romio_ds_read.c_str()));
    }
    if (!Hints::cb_buffer_size.empty()) {
        MPI_Info_set(info, "cb_buffer_size",
                const_cast<char*>(Hints::cb_buffer_size.c_str()));
    }
    if (!Hints::romio_no_indep_rw.empty()) {
        MPI_Info_set(info, "romio_no_indep_rw",
                const_cast<char*>(Hints::romio_no_indep_rw.c_str()));
    }

    return info;
}


string Hints::to_string()
{
    string hints;
    hints += "striping_unit=" + Hints::striping_unit;
    hints += "\n";
    hints += "cb_buffer_size=" + Hints::cb_buffer_size;
    hints += "\n";
    hints += "romio_cb_read=" + Hints::romio_cb_read;
    hints += "\n";
    hints += "romio_ds_read=" + Hints::romio_ds_read;
    hints += "\n";
    hints += "romio_no_indep_rw=" + Hints::romio_no_indep_rw;
    return hints;
}
