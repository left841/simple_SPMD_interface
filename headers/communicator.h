#ifndef __COMMUNICATOR_H__
#define __COMMUNICATOR_H__

#include "mpi.h"
#include "parallel_defs.h"

namespace auto_parallel
{

    class communicator
    {
    protected:

        MPI_Comm comm;
        process comm_rank;
        int comm_size;
        bool created;

        communicator(MPI_Comm _comm);
        explicit communicator(const communicator& c);
        communicator(const communicator& c, int color, int key);

    public:

        ~communicator();

        process rank() const;
        MPI_Comm get_comm() const;
        int size() const;

    };

}

#endif // __COMMUNICATOR_H__
