#ifndef PROCESSGROUP_H_
#define PROCESSGROUP_H_

#include <mpi.h>

#include <vector>

using std::vector;

/**
 * Represents a group of processors (e.g. an MPI_Comm).
 *
 * ProcessGroup creation is collective and resembles the MPI_Comm_split()
 * function. ProcessGroup instances can be optionally passed during Dataset
 * construction. There is always a World ProcessGroup instace.
 */
class ProcessGroup
{
    public:

        /**
         * ProcessGroup creation taking a non-negative group id.
         *
         * @param[in] id of the group
         */
        ProcessGroup(int color);

        /**
         * Return the ID of the group.
         *
         * If Global Arrays is used, this is the process group handle. If
         * Global Arrays is not used, this is a non-negative integer such that
         * all groups have a unique ID.
         */
        int get_id() const;

        /**
         * Return the MPI_Comm instance associated with the group.
         */
        MPI_Comm get_comm() const;

        /**
         * Return the ordered set of ranks associated with this group.
         *
         * The ranks are with respect to the world group.
         */
        const vector<int>& get_ranks() const;

        /**
         * Return the number of processes within this group.
         */
        int get_size() const;

        /**
         * Return the rank of this process with respect to the group size.
         */
        int get_rank() const;

        /**
         * Return the world ProcessGroup.
         */
        static ProcessGroup get_world();

    protected:
        int id;
        MPI_Comm comm;
        vector<int> ranks;
        int rank;
        int size;
};

#endif /* PROCESSGROUP_H_ */
