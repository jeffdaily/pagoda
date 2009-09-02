#include <cstring> // for memset
#include <map>

#include <ga.h>

#include "Common.H"
#include "ConnectivityVariable.H"
#include "Debug.H"
#include "Dimension.H"
#include "DistributedMask.H"

using std::map;


ConnectivityVariable::ConnectivityVariable(Variable *var, Dimension *to)
    :   VariableDecorator(var)
    ,   to(to)
{
}


ConnectivityVariable::~ConnectivityVariable()
{
    to = NULL;
}


Dimension* ConnectivityVariable::get_from_dim() const
{
    return var->get_dims()[0];
}


Dimension* ConnectivityVariable::get_to_dim() const
{
    return to;
}


/**
 * Reassign values of this variable based on its associated masks.
 *
 * Assumes a connectivity variable is an integer variable mapping from one
 * index location to another.
 *
 * TODO ARG!!  The whole int vs Integer vs int64 vs nc_int is frustrating!
 * nc_int is 4-bytes in netcdf 3.6.x, right? Which correspondes to MT_INT on
 * a 4-byte integer platform (i.e. linux). I doubt I got this interplay
 * correct...
 */
void ConnectivityVariable::reindex()
{
    TRACER("ConnectivityVariable::reindex BEGIN\n");
    if (to->get_mask() && to->get_mask()->was_cleared()) {
        to->get_mask()->reindex();
        DistributedMask *mask = dynamic_cast<DistributedMask*>(to->get_mask());
        if (mask) {
            int me = GA_Nodeid();
            int var_handle = var->get_handle();
            size_t var_ndim = var->num_dims();
            int64_t *var_lo = new int64_t[var_ndim];
            int64_t *var_hi = new int64_t[var_ndim];
            int64_t *var_ld = NULL;
            int idx_handle = mask->get_handle_index();
            int idx_type;
            int idx_ndim;
            int64_t idx_size;

            NGA_Inquire64(idx_handle, &idx_type, &idx_ndim, &idx_size);
            if (1 != idx_ndim) {
                GA_Error("ConnectivityVariable::reindex() 1 != idx_ndim",
                        idx_ndim);
            }

            //var->read(); // assumes already read elsewhere
            if (1 < var_ndim) {
                var_ld = new int64_t[var_ndim-1];
            }

            NGA_Distribution64(var_handle, me, var_lo, var_hi);
            if (0 > var_lo[0] && 0 > var_hi[0]);
            else {
                // This is screwy.
                // For each value in the variable we're remapping, we need its
                // associated remapped value based on the mask's reindex array.
                // But the variable's values are not monotonic or anything nice
                // like that, so using NGA_Gather seems like the only way to
                // keep from remotely retrieving more data than we really need.
                // Furthermore, the set of this variable's values that this
                // process manages could contain duplicates. Rather than gather
                // duplicates, we use the C++ map to manage what we actually
                // request. Lastly, the C interface for NGA_Gather is retarded
                // because it requires an array of pointers rather than a flat
                // array of indices. (And internally, GA just transforms the
                // array of pointers into a flat array... WTF)
                int *buf;
                int64_t count;
                map<int,int> m;
                map<int,int>::const_iterator m_it;
                map<int,int>::const_iterator m_end;
                int64_t **subs;
                size_t subs_idx;
                int *values;
                int64_t size;

                TRACER("count how many values are in buf\n");
                count = 1;
                for (size_t i=0; i<var_ndim; ++i) {
                    count *= var_hi[i] - var_lo[i] + 1;
                }
                TRACER("create the map of dim indices we want\n");
                NGA_Access64(var_handle, var_lo, var_hi, &buf, var_ld);
                for (int64_t i=0; i<count; ++i) {
                    m[buf[i]] = -1;
                }
                NGA_Release64(var_handle, var_lo, var_hi);
                size = m.size();
                TRACER1("size = %lld\n", size);

                TRACER("create the retarded array of pointers that GA wants\n");
                subs = new int64_t*[size];
                subs_idx = 0;
                m_it = m.begin();
                m_end = m.end();
                for (; m_it!=m_end; ++m_it) {
                    subs[subs_idx++] = new int64_t(m_it->first);
                    if (0 > subs[subs_idx-1][0]
                            || idx_size <= subs[subs_idx-1][0]) {
                        GA_Error("ConnectivityVariable::reindex out of bounds",
                                subs_idx-1);
                    }
                }

                TRACER("do the Gather\n");
                values = new int[size];
                (void)memset(values, 0, size*sizeof(int));
                NGA_Gather64(idx_handle, values, subs, size);

                TRACER("use the gathered values for the map\n");
                for (int64_t i=0; i<size; ++i) {
                    int64_t val_to_find = subs[i][0];
                    map<int,int>::iterator found = m.find(val_to_find);
                    found->second = values[i];
                    //m.find(subs[i][0])->second = values[i];
                }

                TRACER("now go through buf one last time and replace values!\n");
                NGA_Access64(var_handle, var_lo, var_hi, &buf, var_ld);
                for (int64_t i=0; i<count; ++i) {
                    buf[i] = m.find(buf[i])->second;
                }
                NGA_Release_update64(var_handle, var_lo, var_hi);

                // clean up!
                delete [] values;
                for (int64_t idx=0; idx<size; ++idx) {
                    delete subs[idx];
                }
                delete [] subs;
            }

            if (var_ld) {
                delete [] var_ld;
            }
            delete [] var_hi;
            delete [] var_lo;
        }
    }
    TRACER("ConnectivityVariable::reindex END\n");
}


ostream& ConnectivityVariable::print(ostream &os) const
{
    const string name = var->get_name();
    return os << "ConnectivityVariable(" << name << ")";
}

