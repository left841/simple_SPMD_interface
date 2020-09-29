#include "ready_transfer.h"

namespace apl
{

    ready_sender::ready_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q) : sender(), comm(_comm), proc(_proc), q(_q)
    { }

    void ready_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        int ret = MPI_SUCCESS;
        try
        {
            ret = MPI_Rsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
    }

    MPI_Request ready_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        int ret = MPI_SUCCESS;
        MPI_Request req = MPI_REQUEST_NULL;
        try
        {
            ret = MPI_Irsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
        return req;
    }

    void ready_sender::wait_all() const
    {
        while (!q->empty())
        {
            MPI_Wait(&q->front(), MPI_STATUS_IGNORE);
            q->pop();
        }
    }

    void ready_sender::store_request(MPI_Request req) const
    { q->push(req); }

}
