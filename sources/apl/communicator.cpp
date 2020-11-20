#include "apl/communicator.h"

namespace apl
{

    communicator::communicator(MPI_Comm _comm): comm(_comm)
    {
        created = false;
        apl_MPI_CHECKER(MPI_Comm_rank(comm, &comm_rank));
        apl_MPI_CHECKER(MPI_Comm_size(comm, &comm_size));
    }

    communicator::communicator(const communicator& c)
    {
        apl_MPI_CHECKER(MPI_Comm_dup(c.comm, &comm));
        created = true;
        apl_MPI_CHECKER(MPI_Comm_rank(comm, &comm_rank));
        apl_MPI_CHECKER(MPI_Comm_size(comm, &comm_size));
    }

    communicator::communicator(const communicator& c, int color, int key)
    {
        apl_MPI_CHECKER(MPI_Comm_split(c.comm, color, key, &comm));
        created = true;
        apl_MPI_CHECKER(MPI_Comm_rank(comm, &comm_rank));
        apl_MPI_CHECKER(MPI_Comm_size(comm, &comm_size));
    }

    communicator::~communicator()
    {
        if (created)
            apl_MPI_CHECKER(MPI_Comm_free(&comm));
    }

    int communicator::rank() const
    { return comm_rank; }

    MPI_Comm communicator::get_comm() const
    { return comm; }

    int communicator::size() const
    { return comm_size; }

}
