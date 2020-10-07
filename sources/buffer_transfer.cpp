#include "buffer_transfer.h"

namespace apl
{

    buffer_sender::buffer_sender(MPI_Comm _comm, process _proc, std::vector<MPI_Request>* _q): sender(), comm(_comm), proc(_proc), q(_q)
    { }

    void buffer_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        int ret = MPI_SUCCESS;
        try
        {
            ret = MPI_Bsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
    }

    MPI_Request buffer_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        int ret = MPI_SUCCESS;
        MPI_Request req = MPI_REQUEST_NULL;
        try
        {
            ret = MPI_Ibsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
        return req;
    }

    void buffer_sender::store_request(MPI_Request req) const
    { q->push_back(req); }

}
