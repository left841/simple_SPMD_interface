#include "apl/intracomm.h"

namespace apl
{

    intracomm::intracomm(): communicator()
    { }

    intracomm::intracomm(const intracomm& c): communicator(c)
    { }

    intracomm::intracomm(const intracomm& c, int color, int key)
    { split(c, color, key); }

    intracomm::~intracomm()
    { }

    void intracomm::split(const intracomm& c, int color, int key)
    { apl_MPI_CHECKER(MPI_Comm_split(c.comm, color, key, &comm)); }

    void intracomm::barrier() const
    { apl_MPI_CHECKER(MPI_Barrier(comm)); }

}
