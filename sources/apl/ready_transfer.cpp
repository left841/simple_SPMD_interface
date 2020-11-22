#include "apl/ready_transfer.h"

namespace apl
{

    ready_sender::ready_sender(MPI_Comm _comm, process _proc): sender(), comm(_comm), proc(_proc)
    { }

    void ready_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    { apl_MPI_CHECKER(MPI_Rsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm)); }

    MPI_Request ready_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Irsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

}
