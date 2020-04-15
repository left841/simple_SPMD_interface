#include "buffer_transfer.h"

namespace apl
{

    buffer_sender::buffer_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q): sender(), comm(_comm), proc(_proc), q(_q)
    { }

    void buffer_sender::send(const void* buf, int size, MPI_Datatype type) const
    { MPI_Bsend(buf, size, type, proc, tag++, comm); }

    void buffer_sender::isend(const void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Ibsend(buf, size, type, proc, tag++, comm, &req);
        q->push(req);
    }

    void buffer_sender::wait_all() const
    {
        while (!q->empty())
        {
            MPI_Wait(&q->front(), MPI_STATUS_IGNORE);
            q->pop();
        }
    }

}
