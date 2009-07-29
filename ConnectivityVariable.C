#include "ConnectivityVariable.H"
#include "Dimension.H"
#include "DistributedMask.H"
#include <ga.h>


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
    if (to->get_mask() && to->get_mask()->was_cleared()) {
        to->get_mask()->reindex();
        DistributedMask *mask = dynamic_cast<DistributedMask*>(to->get_mask());
        if (mask) {
            int me = GA_Nodeid();
            int handle = var->get_handle();
            size_t ndim = var->num_dims();
            int *var_lo = new int[ndim];
            int *var_hi = new int[ndim];
            int *var_ld = NULL;
            int *buf = NULL;
            int *gather = NULL;
            int **subs = NULL;
            int count = 1;

            //var->read(); // assumes already read elsewhere
            if (1 < ndim) {
                var_ld = new int[ndim-1];
            }

            NGA_Distribution(handle, me, var_lo, var_hi);
            if (0 > var_lo[0] && 0 > var_hi[0]);
            else {
                for (size_t i=0; i<ndim; ++i) {
                    count *= var_hi[i] - var_lo[i] + 1;
                }
                subs = new int*[count];
                gather = new int[count];

                NGA_Access(handle, var_lo, var_hi, &buf, var_ld);
                for (size_t i=0; i<count; ++i) {
                    subs[i] = &(buf[i]);
                }
                NGA_Gather(mask->get_handle_index(), gather, subs, count);
                // TODO memcpy instead??
                for (size_t i=0; i<count; ++i) {
                    buf[i] = gather[i];
                }
                NGA_Release_update(handle, var_lo, var_hi);

                delete [] gather;
                delete [] subs;
            }

            delete [] var_ld;
            delete [] var_hi;
            delete [] var_lo;
        }
    }
}


ostream& ConnectivityVariable::print(ostream &os) const
{
    return os << "ConnectivityVariable(" << var->get_name() << ")";
}

