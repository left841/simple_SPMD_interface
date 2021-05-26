#include "apl/communicator.h"
#include "apl/comm_group.h"

namespace apl
{

    communicator::communicator(): comm(MPI_COMM_NULL)
    { }

    communicator::communicator(const communicator& c)
    { dublicate(c); }

    communicator::communicator(communicator && c) noexcept: comm(std::exchange(c.comm, MPI_COMM_NULL))
    { }

    communicator& communicator::operator=(const communicator& c)
    {
        if (this != &c)
        {
            if (comm != MPI_COMM_NULL)
                free();
            dublicate(c);
        }
        return *this;
    }

    communicator& communicator::operator=(communicator&& c) noexcept
    {
        if (this != &c)
        {
            if (comm != MPI_COMM_NULL)
                free();
            comm = std::exchange(c.comm, MPI_COMM_NULL);
        }
        return *this;
    }

    communicator::~communicator()
    {
        if (comm != MPI_COMM_NULL)
            free();
    }

    void communicator::assign(MPI_Comm _comm)
    { comm = _comm; }

    void communicator::unassign()
    { comm = MPI_COMM_NULL; }

    void communicator::dublicate(const communicator& c)
    { apl_MPI_CHECKER(MPI_Comm_dup(c.comm, &comm)); }

    void communicator::free()
    {
        apl_MPI_CHECKER(MPI_Comm_free(&comm));
        comm = MPI_COMM_NULL;
    }

    void communicator::abort(int err) const
    { apl_MPI_CHECKER(MPI_Abort(comm, err)); }

    process communicator::rank() const
    {
        process comm_rank = MPI_PROC_NULL;
        apl_MPI_CHECKER(MPI_Comm_rank(comm, &comm_rank));
        return comm_rank;
    }

    MPI_Comm communicator::get_comm() const
    { return comm; }

    int communicator::size() const
    {
        int comm_size = 0;
        apl_MPI_CHECKER(MPI_Comm_size(comm, &comm_size));
        return comm_size;
    }

    comm_group communicator::group() const
    {
        comm_group g(*this);
        return g;
    }

    bool communicator::valid() const
    { return comm != MPI_COMM_NULL; }

}
