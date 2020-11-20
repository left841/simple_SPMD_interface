#include "apl/buffer_transfer.h"

namespace apl
{

    buffer_sender::buffer_sender(MPI_Comm _comm, process _proc): sender(), comm(_comm), proc(_proc)
    { }

    buffer_sender::buffer_sender(MPI_Comm _comm, process _proc, request_block& _req): sender(_req), comm(_comm), proc(_proc)
    { }

    void buffer_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    { apl_MPI_CHECKER(MPI_Bsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm)); }

    MPI_Request buffer_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Ibsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

}
