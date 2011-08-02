#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif

#include <vector>

using std::vector;

#include <mpi.h>
#if HAVE_GA
#   include <ga.h>
#endif

#include "ProcessGroup.H"

ProcessGroup::ProcessGroup(int id)
    :   id(id)
    ,   comm(MPI_COMM_NULL)
    ,   ga_id(GA_Pgroup_get_world())
    ,   ranks()
    ,   size(0)
{
    if (id < 0) {
        MPI_Comm_dup(MPI_COMM_WORLD, &comm);
        MPI_Comm_size(comm, &size);
        ranks.resize(size);
        for (int i=0; i<size; ++i) {
            ranks[i] = i;
        }
    } else {
        int rank;
        MPI_Comm_split(get_world().get_comm(), id, 0, &comm);
        MPI_Comm_size(comm, &size);
        ranks.resize(size);
        MPI_Comm_rank(get_world().get_comm(), &rank);
        MPI_Allgather(&rank, 1, MPI_INT, &ranks[0], 1, MPI_INT, comm);
    }
}


int ProcessGroup::get_id() const
{
    return id;
}


MPI_Comm ProcessGroup::get_comm() const
{
    return comm;
}


int ProcessGroup::get_ga_group() const
{
    return ga_id;
}


ProcessGroup ProcessGroup::get_world()
{
    static ProcessGroup inst(-1);
    return inst;
}
