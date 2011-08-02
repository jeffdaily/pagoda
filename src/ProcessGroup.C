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

#include "ProcessGroup.H"

ProcessGroup::ProcessGroup(int color)
    :   id(color)
    ,   comm(MPI_COMM_NULL)
    ,   ranks()
    ,   rank(-1)
    ,   size(-1)
{
    if (color < 0) {
        MPI_Comm_dup(MPI_COMM_WORLD, &comm);
        MPI_Comm_size(comm, &size);
        MPI_Comm_rank(comm, &rank);
        ranks.resize(size);
        for (int i=0; i<size; ++i) {
            ranks[i] = i;
        }
#if HAVE_GA
        id = GA_Pgroup_get_world();
#else
        id = -1;
#endif
    } else {
        int world_rank;

        MPI_Comm_split(get_world().get_comm(), color, 0, &comm);
        MPI_Comm_size(comm, &size);
        MPI_Comm_rank(comm, &rank);
        MPI_Comm_rank(get_world().get_comm(), &world_rank);
        ranks.resize(size);
        MPI_Allgather(&rank, 1, MPI_INT, &ranks[0], 1, MPI_INT, comm);
#if HAVE_GA
        // GA pgroup creation is collective, so we need one create call per
        // process group id and each process must have the same list of
        // process IDs.
        // gather list of group IDs
        vector<int> group_ids_all(get_world().get_size());
        vector<int> group_ids(get_world().get_size());
        vector<int>::iterator it;

        MPI_Allgather(&color, 1, MPI_INT, &group_ids_all[0], 1, MPI_INT,
                get_world().get_comm());
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
            }
        }
#endif
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


const vector<int>& ProcessGroup::get_ranks() const
{
    return ranks;
}


int ProcessGroup::get_size() const
{
    return size;
}


int ProcessGroup::get_rank() const
{
    return rank;
}


ProcessGroup ProcessGroup::get_world()
{
    static ProcessGroup inst(-1);
    return inst;
}
