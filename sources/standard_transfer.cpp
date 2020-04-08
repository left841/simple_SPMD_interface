#include "standard_transfer.h"

namespace apl
{

    standard_sender::standard_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q): sender(), comm(_comm), proc(_proc), q(_q)
    { }

    void standard_sender::send(const void* buf, int size, MPI_Datatype type) const
    { MPI_Send(buf, size, type, proc, tag++, comm); }

    void standard_sender::isend(const void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Isend(buf, size, type, proc, tag++, comm, &req);
        q->push(req);
    }

    standard_receiver::standard_receiver(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q): receiver(), comm(_comm), proc(_proc), q(_q)
    { tag = 0; }

    void standard_receiver::recv(void* buf, int size, MPI_Datatype type) const
    { MPI_Recv(buf, size, type, proc, tag++, comm, MPI_STATUS_IGNORE); }

    void standard_receiver::irecv(void* buf, int size, MPI_Datatype type) const
    {
        MPI_Request req;
        MPI_Irecv(buf, size, type, proc, tag++, comm, &req);
        q->push(req);
    }

    int standard_receiver::probe(MPI_Datatype type) const
    {
        MPI_Status status;
        int size;
        MPI_Probe(proc, tag, comm, &status);
        MPI_Get_count(&status, type, &size);
        return size;
    }

}
