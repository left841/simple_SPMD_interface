#include "ready_transfer.h"

namespace auto_parallel
{

    ready_sender::ready_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q) : sender(), comm(_comm), proc(_proc), q(_q)
    { }

    void ready_sender::send(const void* buf, int size, MPI_Datatype type) const
    {
        MPI_Rsend(buf, size, type, proc, tag++, comm);
    }

    void ready_sender::isend(const void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Irsend(buf, size, type, proc, tag++, comm, &req);
        q->push(req);
    }

}
