#include "apl/standard_transfer.h"

namespace apl
{

    standard_sender::standard_sender(MPI_Comm _comm, process _proc): sender(), comm(_comm), proc(_proc)
    { }

    void standard_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    { apl_MPI_CHECKER(MPI_Send(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm)); }

    MPI_Request standard_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Isend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

    standard_receiver::standard_receiver(MPI_Comm _comm, process _proc): receiver(), comm(_comm), proc(_proc)
    { }

    MPI_Status standard_receiver::recv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const
    {
        MPI_Status status {};
        apl_MPI_CHECKER(MPI_Recv(buf, static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &status));
        return status;
    }

    MPI_Request standard_receiver::irecv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Irecv(buf, static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

    MPI_Status standard_receiver::probe_impl(TAG tg) const
    {
        MPI_Status status {};
        apl_MPI_CHECKER(MPI_Probe(proc, static_cast<int>(tg), comm, &status));
        return status;
    }

}
