#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif

#include <algorithm>
#include <vector>

using std::sort;
using std::unique;
using std::vector;

#include <mpi.h>
#if HAVE_GA
#   include <ga.h>
#endif

#include "Bootstrap.H"
#include "Error.H"
#include "Print.H"
#include "ProcessGroup.H"


ProcessGroup ProcessGroup::default_group;


ProcessGroup::ProcessGroup()
    :   id(-1)
    ,   comm(MPI_COMM_NULL)
    ,   ranks()
    ,   rank(-1)
    ,   size(-1)
{
    if (pagoda::COMM_WORLD != MPI_COMM_NULL) {
        comm = pagoda::COMM_WORLD;
        rank = pagoda::me;
        size = pagoda::npe;
        ranks.resize(size);
        for (int i=0; i<size; ++i) {
            ranks[i] = i;
        }
    }
}


#if HAVE_GA
extern "C" MPI_Comm ga_mpi_pgroup_communicator(int p_grp);
#endif

ProcessGroup::ProcessGroup(int color)
    :   id(color)
    ,   comm(MPI_COMM_NULL)
    ,   ranks()
    ,   rank(-1)
    ,   size(-1)
{
#if HAVE_GA
    // GA pgroup creation is collective, so we need one create call per
    // process group id and each process must have the same list of
    // process IDs.
    // gather list of group IDs
    vector<int> group_ids_all(pagoda::npe);
    vector<int> group_ids(pagoda::npe);
    vector<int>::iterator it;
    ProcessGroup *default_to_set = NULL;

    MPI_Allgather(&color, 1, MPI_INT, &group_ids_all[0], 1, MPI_INT,
            pagoda::COMM_WORLD);
    group_ids.assign(group_ids_all.begin(), group_ids_all.end());
    sort(group_ids.begin(), group_ids.end());
    it = unique(group_ids.begin(), group_ids.end());
    group_ids.resize(it - group_ids.begin());
    for (size_t i=0; i<group_ids.size(); ++i) {
        int tmp_ga_id;
        int group_id = group_ids[i];
        vector<int> the_list;
        for (size_t j=0; j<group_ids_all.size(); ++j) {
            if (group_ids_all[j] == group_id) {
                the_list.push_back(j);
            }
        }
        tmp_ga_id = GA_Pgroup_create(&the_list[0], the_list.size());
        if (group_id == color) {
            id = tmp_ga_id;
            comm = ga_mpi_pgroup_communicator(id);
            default_to_set = this;
        }
    }
    set_default(*default_to_set);
#else
    int world_rank;
    int mpierr;

    ASSERT(color >= 0);
    mpierr = MPI_Comm_split(pagoda::COMM_WORLD, color, 0, &comm);
    if (mpierr != MPI_SUCCESS) {
        if (pagoda::me == 0) {
            printf("MPI_Comm_split failed, aborting\n");
        }
        MPI_Abort(pagoda::COMM_WORLD, mpierr);
    }
    MPI_Comm_size(comm, &size);
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_rank(pagoda::COMM_WORLD, &world_rank);
    ranks.resize(size);
    MPI_Allgather(&rank, 1, MPI_INT, &ranks[0], 1, MPI_INT, comm);
#endif
}


int ProcessGroup::get_id() const
{
    return id;
}


MPI_Comm ProcessGroup::get_comm() const
{
    return comm;
}


const vector<int>& ProcessGroup::get_ranks() const
{
    return ranks;
}


int ProcessGroup::get_rank() const
{
    return rank;
}


int ProcessGroup::get_size() const
{
    return size;
}


void ProcessGroup::barrier() const
{
#if HAVE_GA
    GA_Pgroup_sync(id);
#else
    MPI_Barrier(comm);
#endif
}


ProcessGroup ProcessGroup::get_world()
{
    static ProcessGroup inst;
    return inst;
}


void ProcessGroup::set_default(const ProcessGroup &group)
{
    ProcessGroup::default_group = group;
#if HAVE_GA
    GA_Pgroup_set_default(group.get_id());
#endif
}


ProcessGroup ProcessGroup::get_default()
{
    return ProcessGroup::default_group;
}
