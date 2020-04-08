#include "synchronous_transfer.h"

namespace auto_parallel
{

    synchronous_sender::synchronous_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q): sender(), comm(_comm), proc(_proc), q(_q)
    { }

    void synchronous_sender::send(const void* buf, int size, MPI_Datatype type) const
    { MPI_Ssend(buf, size, type, proc, tag++, comm); }

    void synchronous_sender::isend(const void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Issend(buf, size, type, proc, tag++, comm, &req);
        q->push(req);
    }

}
