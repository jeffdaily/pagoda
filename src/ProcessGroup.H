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
        ProcessGroup(int id);

        /**
         * Return the ID used to create the group.
         */
        int get_id() const;

        /**
         * Return the MPI_Comm instance associated with the group.
         */
        MPI_Comm get_comm() const;

        /**
         * Return the Global Arrays group ID associated with the group.
         */
        int get_ga_group() const;

        /**
         * Return the world ProcessGroup.
         */
        static ProcessGroup get_world();

    protected:
        int id;
        MPI_Comm comm;
        int ga_id;
        vector<int> ranks;
        int size;
};

#endif /* PROCESSGROUP_H_ */
