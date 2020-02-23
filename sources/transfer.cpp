#include "transfer.h"

namespace auto_parallel
{

    sender::sender(MPI_Comm _comm, int _proc, std::queue<MPI_Request>* _q): comm(_comm), proc(_proc), q(_q)
    { tag = 0; }

    void sender::send(void* buf, int size, MPI_Datatype type) const
    { MPI_Send(buf, size, type, proc, tag++, comm); }

    void sender::isend(void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, type, proc, tag++, comm, &req);
        q->push(req);
    }

    receiver::receiver(MPI_Comm _comm, int _proc, std::queue<MPI_Request>* _q): comm(_comm), proc(_proc), q(_q)
    { tag = 0; }

    void receiver::recv(void* buf, int size, MPI_Datatype type) const
    { MPI_Recv(buf, size, type, proc, tag++, comm, MPI_STATUS_IGNORE); }

    void receiver::irecv(void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, type, proc, tag++, comm, &req);
        q->push(req);
    }

    int receiver::probe(MPI_Datatype type) const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, type, &size);
        return size;
    }

}
