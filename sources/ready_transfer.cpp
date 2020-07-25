#include "ready_transfer.h"

namespace apl
{

    ready_sender::ready_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q) : sender(), comm(_comm), proc(_proc), q(_q)
    { }

    void ready_sender::send(const void* buf, int size, MPI_Datatype type) const
    {
        MPI_Rsend(const_cast<void*>(buf), size, type, proc, tag++, comm);
    }

    void ready_sender::isend(const void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Irsend(const_cast<void*>(buf), size, type, proc, tag++, comm, &req);
        q->push(req);
    }

    void ready_sender::wait_all() const
    {
        while (!q->empty())
        {
            MPI_Wait(&q->front(), MPI_STATUS_IGNORE);
            q->pop();
        }
    }

}
