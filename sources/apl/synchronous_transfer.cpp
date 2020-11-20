#include "apl/synchronous_transfer.h"

namespace apl
{

    synchronous_sender::synchronous_sender(MPI_Comm _comm, process _proc): sender(), comm(_comm), proc(_proc)
    { }

    synchronous_sender::synchronous_sender(MPI_Comm _comm, process _proc, request_block& _req): sender(_req), comm(_comm), proc(_proc)
    { }

    void synchronous_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    { apl_MPI_CHECKER(MPI_Ssend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm)); }

    MPI_Request synchronous_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Issend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

}
